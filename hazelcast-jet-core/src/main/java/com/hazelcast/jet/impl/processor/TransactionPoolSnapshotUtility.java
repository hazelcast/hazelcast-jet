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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static java.util.Collections.singletonList;

/**
 * A utility to handle transactions where each local processor uses a pool of
 * transactional resources alternately. This is needed if we can't reliably
 * list transactions that were created by our processor and solves it by having
 * a deterministic transaction IDs.
 */
public class TransactionPoolSnapshotUtility<TXN_ID extends TransactionId, RES extends TransactionalResource<TXN_ID>>
        extends TwoPhaseSnapshotCommitUtility<TXN_ID, RES> {

    private static final int TXN_PROBING_FACTOR = 5;
    private final TransactionalResource<TXN_ID> noopTransaction = new TransactionalResource<TXN_ID>() {
        @Override
        public void begin() {
            throw new IllegalStateException("this implementation is not supposed to be begun");
        }

        @Override
        public TXN_ID id() {
            return null;
        }

        @Override
        public void commit() {
        }
    };

    private final int poolSize;
    private final List<TXN_ID> transactionIds;
    private List<RES> transactions;
    private int activeTxnIndex;
    private boolean transactionBegun;
    private boolean snapshotInProgress;
    private TransactionalResource<TXN_ID> transactionToCommit;
    private boolean flushed;
    private boolean processorCompleted;
    private boolean transactionsReleased;

    /**
     * @param createTxnIdFn input is {processorIndex, transactionIndex}
     */
    public TransactionPoolSnapshotUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            boolean isSource,
            @Nonnull ProcessingGuarantee externalGuarantee,
            int poolSize,
            @Nonnull BiFunctionEx<Integer, Integer, TXN_ID> createTxnIdFn,
            @Nonnull FunctionEx<TXN_ID, RES> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndAbortFn
    ) {
        super(outbox, procContext, isSource, externalGuarantee, createTxnFn, recoverAndCommitFn,
                processorIndex -> {
                    for (int i = 0; i < adjustPoolSize(externalGuarantee, isSource, poolSize); i++) {
                        TXN_ID txnId = createTxnIdFn.apply(processorIndex, i);
                        LoggingUtil.logFine(procContext.logger(), "recover and abort %s", txnId);
                        recoverAndAbortFn.accept(txnId);
                    }
                });

        this.poolSize = adjustPoolSize(externalGuarantee, isSource, poolSize);
        LoggingUtil.logFine(procContext.logger(), "Actual pool size used: %d", this.poolSize);
        if (this.poolSize > 1) {
            transactionIds = new ArrayList<>(this.poolSize);
            for (int i = 0; i < this.poolSize; i++) {
                transactionIds.add(createTxnIdFn.apply(procContext().globalProcessorIndex(), i));
                assert i == 0 || !transactionIds.get(i).equals(transactionIds.get(i - 1)) : "two equal IDs generated";
            }
        } else {
            transactionIds = singletonList(null);
        }
    }

    private static int adjustPoolSize(@Nonnull ProcessingGuarantee externalGuarantee, boolean isSource, int poolSize) {
        // we need at least 1 transaction or 2 for ex-once. More than 3 is never needed.
        if (externalGuarantee == EXACTLY_ONCE && poolSize < 2 || poolSize < 1 || poolSize > 3) {
            throw new IllegalArgumentException("poolSize=" + poolSize);
        }
        // for at-least-once source we don't need more than two transactions
        if (externalGuarantee == AT_LEAST_ONCE && isSource) {
            poolSize = Math.min(2, poolSize);
        }
        // for no guarantee and for an at-least-once sink we need just 1 txn
        if (externalGuarantee == NONE || externalGuarantee == AT_LEAST_ONCE && !isSource) {
            poolSize = 1;
        }
        return poolSize;
    }

    @Nullable @Override
    public RES activeTransaction() {
        if (transactions == null) {
            if (transactionsReleased) {
                throw new IllegalStateException("transactions already released");
            }
            rollbackOtherTransactions();
            transactions = transactionIds.stream().map(createTxnFn()).collect(Collectors.toList());
        }
        if (usesTransactionLifecycle() && poolSize < (snapshotInProgress ? 3 : 2)) {
            return null;
        }
        RES activeTransaction = transactions.get(activeTxnIndex);
        if (!transactionBegun && usesTransactionLifecycle()) {
            try {
                LoggingUtil.logFine(procContext().logger(), "begin, txnId=%s", activeTransaction.id());
                activeTransaction.begin();
            } catch (Exception e) {
                throw new JetException("Transaction begin failed: " + e + ", txnId=" + activeTransaction.id());
            }
        }
        transactionBegun = true;
        return activeTransaction;
    }

    private void rollbackOtherTransactions() {
        if (!usesTransactionLifecycle()) {
            return;
        }
        // If a member is removed or the local parallelism is reduced, the
        // transactions with higher processor index won't be used. We need to
        // roll these back too. We probe transaction IDs with processorIndex
        // beyond those of the current execution, up to 5x (the
        // TXN_PROBING_FACTOR) of the current total parallelism. We only roll
        // back "our" transactions, that is those where index%parallelism =
        // ourIndex
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
        if (externalGuarantee() == NONE) {
            return true;
        }
        assert !snapshotInProgress : "snapshot in progress";
        // use no-op transaction if the active one wasn't begun
        transactionToCommit = transactionBegun ? activeTransaction() : noopTransaction;
        assert transactionToCommit != null : "transactionToCommit == null, transactionBegun=" + transactionBegun;
        try {
            // `flushed` is used to avoid double flushing
            if (!flushed) {
                LoggingUtil.logFine(procContext().logger(), "flush, txnId=%s", transactionToCommit.id());
                if (!transactionToCommit.flush()) {
                    procContext().logger().fine("flush returned false");
                    return false;
                }
            }
            flushed = true;
            if (usesTransactionLifecycle()) {
                TXN_ID txnId = transactionToCommit.id();
                if (txnId != null && !getOutbox().offerToSnapshot(broadcastKey(txnId), false)) {
                    return false;
                }
                LoggingUtil.logFine(procContext().logger(), "endAndPrepare, txnId=%s", transactionToCommit.id());
                transactionToCommit.endAndPrepare();
                transactionBegun = false;
            }
            incrementActiveTxnIndex();
        } catch (Exception e) {
            throw new JetException("Save to snapshot failed: " + e + ", txnId=" + transactionToCommit.id());
        }
        snapshotInProgress = true;
        flushed = false;
        return true;
    }

    @Override
    public boolean onSnapshotCompleted(boolean commitTransactions) {
        if (!usesTransactionLifecycle()) {
            return true;
        }
        assert snapshotInProgress : "snapshot not in progress";
        snapshotInProgress = false;
        if (commitTransactions) {
            try {
                LoggingUtil.logFine(procContext().logger(), "commit, txnId=%s", transactionToCommit.id());
                transactionToCommit.commit();
            } catch (Exception e) {
                throw new JetException("Transaction commit failed: " + e + ", txnId=" + transactionToCommit.id());
            }
        } else {
            // we can't ignore the snapshot failure
            throw new RetryableHazelcastException("the snapshot failed");
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
        TXN_ID txnId = ((BroadcastKey<TXN_ID>) key).key();
        if (externalGuarantee() == EXACTLY_ONCE
                && txnId.index() % procContext().totalParallelism() == procContext().globalProcessorIndex()) {
            if (transactionIds.get(0).equals(txnId)) {
                // If we restored txnId of the 0th transaction, make the other transaction active.
                // We must avoid using the same transaction that we committed in the snapshot
                // we're restoring from, because if the job fails without creating a snapshot, we
                // would commit the transaction that should be rolled back.
                // Note that we can restore a TxnId that is neither of our current two IDs in case
                // the job is upgraded and has a new jobId.
                incrementActiveTxnIndex();
            }
            LoggingUtil.logFine(procContext().logger(), "recover and commit %s", txnId);
            recoverAndCommitFn().accept(txnId);
        }
    }

    @Override
    public void close() {
        doRelease();
    }

    private void incrementActiveTxnIndex() {
        activeTxnIndex++;
        if (activeTxnIndex == poolSize) {
            activeTxnIndex = 0;
        }
    }

    private void doRelease() {
        if (transactionsReleased) {
            return;
        }
        transactionsReleased = true;
        if (transactions != null) {
            for (RES txn : transactions) {
                try {
                    LoggingUtil.logFine(procContext().logger(), "release, txnId=%s", txn.id());
                    txn.release();
                } catch (Exception e) {
                    throw new JetException("Releasing of transaction failed: " + e + ", txnId=" + txn.id(), e);
                }
            }
        }
    }
}
