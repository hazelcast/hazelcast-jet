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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
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
import java.util.Map.Entry;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

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

    private final List<TXN> transactions = Arrays.asList(null, null);
    private int activeTransactionIndex;
    private boolean transactionBegun;
    private boolean snapshotInProgress;
    private boolean processorCompleted;
    private boolean transactionsReleased;

    public TwoTransactionProcessorUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull FunctionEx<Boolean, TXN> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull ConsumerEx<Integer> recoverAndAbortFn
    ) {
        super(outbox, procContext, externalGuarantee, createTxnFn, recoverAndCommitFn, recoverAndAbortFn);
    }

    @Nullable @Override
    public TXN activeTransaction() {
        if (snapshotInProgress) {
            return null;
        }
        TXN activeTransaction = transactions.get(activeTransactionIndex);
        if (activeTransaction == null) {
            if (transactionsReleased) {
                throw new IllegalStateException("transaction already released");
            }
            if (externalGuarantee() == EXACTLY_ONCE) {
                activeTransaction = createTxnFn().apply(true);
                transactions.set(activeTransactionIndex, activeTransaction);
                if (activeTransaction.id().index() != procContext().globalProcessorIndex()) {
                    throw new JetException("The transaction must have an ID.index equal to this processor's global " +
                            "processor index");
                }
            } else {
                activeTransaction = createTxnFn().apply(false);
                transactions.set(activeTransactionIndex, activeTransaction);
            }
        }
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
    public void restoreFromSnapshot(@Nonnull Entry<BroadcastKey<TXN_ID>, Boolean> snapshotEntry) {
        assert transactions.get(0) == null : "transactions already created";
        TXN_ID txnId = snapshotEntry.getKey().key();
        if (txnId.index() % procContext().totalParallelism() == procContext().globalProcessorIndex()) {
            recoverAndCommitFn().accept(txnId);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        // Rollback other transactions. We do this by probing transaction IDs beyond those of the
        // current execution, up to 5x of the current member count.
        for (
                int index = procContext().globalProcessorIndex();
                index < procContext().totalParallelism() * TXN_PROBING_FACTOR;
                index += procContext().totalParallelism()
        ) {
            recoverAndAbortFn().accept(index);
        }
        return true;
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
