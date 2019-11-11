/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.Collections.singletonList;

/**
 * Utility to handle transactions where each local processor uses a pair of
 * transactional resources alternately. This is needed if each resource has a
 * single transaction ID that doesn't change when a new transaction is begun.
 * For this reason we don't process items while waiting for phase-2 - if the
 * onSnapshotCompleted will be called with {@code false}, we'll have 2 pending
 * transactions and would need 3rd one for the next snapshot.
 */
public class TwoTransactionProcessorUtility<TXN_ID extends TransactionId, TXN extends TransactionalResource<TXN_ID>>
        extends TwoPhaseSnapshotCommitUtility<TXN_ID, TXN> {

    private static final int TXN_PROBING_FACTOR = 5;

    private List<TXN> transactions;
    private final List<TXN_ID> transactionIds;
    private int activeTransactionIndex;
    private boolean transactionBegun;
    private boolean snapshotInProgress;
    private boolean processorCompleted;
    private boolean transactionsReleased;

    /**
     * @param createTxnIdFn input is {processorIndex, transactionIndex}
     */
    public TwoTransactionProcessorUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull BiFunctionEx<Integer, Integer, TXN_ID> createTxnIdFn,
            @Nonnull FunctionEx<TXN_ID, TXN> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndAbortFn
    ) {
        super(outbox, procContext, externalGuarantee, createTxnFn, recoverAndCommitFn,
                processorIndex -> {
                    recoverAndAbortFn.accept(createTxnIdFn.apply(processorIndex, 0));
                    recoverAndAbortFn.accept(createTxnIdFn.apply(processorIndex, 1));
                });

        if (externalGuarantee() == EXACTLY_ONCE) {
            transactionIds = Arrays.asList(
                    createTxnIdFn.apply(procContext().globalProcessorIndex(), 0),
                    createTxnIdFn.apply(procContext().globalProcessorIndex(), 1)
            );
            assert !transactionIds.get(0).equals(transactionIds.get(1)) : "two equal IDs generated";
        } else {
            transactionIds = null;
        }
    }

    @Nullable @Override
    public TXN activeTransaction() {
        if (snapshotInProgress) {
            return null;
        }
        if (transactions == null) {
            rollbackOtherTransactions();
            if (transactionsReleased) {
                throw new IllegalStateException("transactions already released");
            }
            if (externalGuarantee() == EXACTLY_ONCE) {
                transactions = transactionIds.stream().map(createTxnFn()).collect(Collectors.toList());
            } else {
                transactions = singletonList(createTxnFn().apply(null));
            }
        }
        TXN activeTransaction = transactions.get(activeTransactionIndex);
        if (externalGuarantee() == EXACTLY_ONCE && !transactionBegun) {
            try {
                activeTransaction.beginTransaction();
                transactionBegun = true;
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
        return activeTransaction;
    }

    private void rollbackOtherTransactions() {
        if (externalGuarantee() == NONE) {
            return;
        }
        // If a member is removed or the local parallelism is reduced, the transactions
        // with higher processor index won't be used. We need to roll them back.
        // We probe transaction IDs beyond those of the current execution, up to 5x
        // (the TXN_PROBING_FACTOR) of the current total parallelism. We only roll
        // back "our" transactions, that is those where index%parallelism = ourIndex
        for (
                int index = procContext().totalParallelism() + procContext().globalProcessorIndex();
                index < procContext().totalParallelism() * TXN_PROBING_FACTOR;
                index += procContext().totalParallelism()
        ) {
            recoverAndAbortFn().accept(index);
        }
    }

    @Override
    public boolean saveToSnapshot() {
        assert !snapshotInProgress : "snapshot in progress";
        // TODO [viliam] avoid doing work if transaction wasn't begun
        TXN activeTransaction = activeTransaction();
        assert activeTransaction != null : "null activeTransaction";
        if (externalGuarantee() == EXACTLY_ONCE) {
            if (!getOutbox().offerToSnapshot(broadcastKey(activeTransaction.id()), false)) {
                return false;
            }
            snapshotInProgress = true;
            transactionBegun = false;
            try {
                activeTransaction.prepare();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
        if (externalGuarantee() == AT_LEAST_ONCE) {
            try {
                activeTransaction.flush();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
        return true;
    }

    @Override
    public boolean onSnapshotCompleted(boolean commitTransactions) {
        snapshotInProgress = false;
        if (externalGuarantee() == EXACTLY_ONCE && commitTransactions) {
            try {
                procContext().logger().finest("commit");
                TXN oldTransaction = transactions.get(activeTransactionIndex);
                oldTransaction.commit();
                procContext().logger().finest("beginTransaction");
                activeTransactionIndex ^= 1;
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
        if (processorCompleted) {
            doRelease();
        }
        return true;
    }

    @Override
    public void afterCompleted() {
        // if the processor completes and a snapshot is in progress, the onSnapshotComplete
        // will be called anyway - we'll not release in that case
        processorCompleted = true;
        if (!snapshotInProgress) {
            doRelease();
        }
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        assert !transactionBegun : "transaction already begun";
        @SuppressWarnings("unchecked")
        TXN_ID txnId = ((BroadcastKey<TXN_ID>)key).key();
        if (externalGuarantee() == EXACTLY_ONCE
                && txnId.index() % procContext().totalParallelism() == procContext().globalProcessorIndex()) {
            if (transactionIds.get(0).equals(txnId)) {
                // If we restored txnId of the 0th transaction, make the other transaction active.
                // We must avoid using the same transaction that we committed in the snapshot
                // we're restoring from, because if the job fails without creating a snapshot, we
                // would commit the transaction that should be rolled back.
                // Note that we can restore a TxnId that is neither of our current two IDs in case
                // the job is upgraded and has a new jobId.
                activeTransactionIndex = 1;
            }
            recoverAndCommitFn().accept(txnId);
        }
    }

    @Override
    public void close() {
        doRelease();
    }

    private void doRelease() {
        if (transactionsReleased) {
            return;
        }
        transactionsReleased = true;
        if (transactions != null) {
            for (TXN txn : transactions) {
                if (txn != null) {
                    try {
                        txn.release();
                    } catch (Exception e) {
                        throw sneakyThrow(e);
                    }
                }
            }
        }
    }
}
