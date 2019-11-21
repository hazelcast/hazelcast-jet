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
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.util.Util.RunnableExc;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Utility to handle transactions in a processor that is able to have unbounded
 * number of open transactions and is able to enumerate those pertaining to a
 * job.
 *
 * @param <TXN> the transaction type
 */
public class UnboundedTransactionsProcessorUtility<TXN_ID extends TransactionId, TXN extends TransactionalResource<TXN_ID>>
        extends TwoPhaseSnapshotCommitUtility<TXN_ID, TXN> {

    private final Supplier<TXN_ID> createTxnIdFn;
    private final RunnableExc abortUnfinishedTransactions;

    private TXN activeTransaction;
    private final Map<TXN_ID, TXN> pendingTransactions = new LinkedHashMap<>();
    private final Queue<TXN_ID> snapshotQueue = new ArrayDeque<>();
    private boolean initialized;
    private boolean waitingForPhase2;

    /**
     * This utility always calls `flush` at most once for each transaction. If
     * transactions aren't used (with externalGuarantee != EXACTLY_ONCE), flush
     * can occur multiple times for the same writer.
     *
     * @param outbox
     * @param procContext
     * @param externalGuarantee
     * @param createTxnFn
     * @param recoverAndCommitFn
     * @param abortUnfinishedTransactions when called, it should abort all
     *      unfinished transactions found in the external system that pertain
     *      to the processor
     */
    public UnboundedTransactionsProcessorUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull Supplier<TXN_ID> createTxnIdFn,
            @Nonnull FunctionEx<TXN_ID, TXN> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull RunnableExc abortUnfinishedTransactions
    ) {
        super(outbox, procContext, false, externalGuarantee, createTxnFn, recoverAndCommitFn,
                txnId -> {
                    throw new UnsupportedOperationException();
                });
        this.createTxnIdFn = createTxnIdFn;
        this.abortUnfinishedTransactions = abortUnfinishedTransactions;
    }

    @Nonnull @Override
    public TXN activeTransaction() {
        if (activeTransaction == null) {
            if (!initialized && externalGuarantee() != NONE) {
                initialized = true;
                try {
                    abortUnfinishedTransactions.run();
                } catch (Exception e) {
                    throw sneakyThrow(e);
                }
            }
            activeTransaction = createTxnFn().apply(createTxnIdFn.get());
            if (externalGuarantee() == EXACTLY_ONCE) {
                try {
                    activeTransaction.begin();
                } catch (Exception e) {
                    throw sneakyThrow(e);
                }
            }
        }
        return activeTransaction;
    }

    public TXN newActiveTransaction() {
        try {
            if (externalGuarantee() == EXACTLY_ONCE) {
                pendingTransactions.put(activeTransaction.id(), activeTransaction);
                activeTransaction.endAndPrepare();
            } else if (activeTransaction != null) {
                activeTransaction.release();
            }
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
        activeTransaction = null;
        return activeTransaction();
    }

    @Override
    public void afterCompleted() {
        if (externalGuarantee() == EXACTLY_ONCE) {
            procContext().logger().info("aaa afterCompleted");
            if (activeTransaction != null) {
                pendingTransactions.put(activeTransaction.id(), activeTransaction);
            }
            if (!waitingForPhase2) {
                for (TXN txn : pendingTransactions.values()) {
                    try {
                        txn.commit();
                        txn.release();
                    } catch (Exception e) {
                        throw sneakyThrow(e);
                    }
                }
            }
        } else {
            try {
                if (activeTransaction != null) {
                    activeTransaction.release();
                }
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
            assert pendingTransactions.isEmpty() : "pendingTransactions not empty";
        }
        activeTransaction = null;
    }

    @Override
    public boolean saveToSnapshot() {
        procContext().logger().info("aaa saveToSnapshot");
        switch (externalGuarantee()) {
            case NONE:
                return true;

            case AT_LEAST_ONCE:
                try {
                    activeTransaction.flush();
                } catch (Exception e) {
                    throw sneakyThrow(e);
                }
                return true;

            case EXACTLY_ONCE:
                waitingForPhase2 = true;
                if (snapshotQueue.isEmpty()) {
                    if (activeTransaction != null) {
                        pendingTransactions.put(activeTransaction.id(), activeTransaction);
                        try {
                            activeTransaction.endAndPrepare();
                        } catch (Exception e) {
                            throw sneakyThrow(e);
                        }
                        activeTransaction = null;
                    }
                    snapshotQueue.addAll(pendingTransactions.keySet());
                }
                for (TXN_ID txnId; (txnId = snapshotQueue.peek()) != null; ) {
                    if (!getOutbox().offerToSnapshot(broadcastKey(txnId), false)) {
                        return false;
                    }
                    snapshotQueue.remove();
                }
                return true;

            default:
                throw new UnsupportedOperationException(externalGuarantee().toString());
        }
    }

    @Override
    public boolean onSnapshotCompleted(boolean commitTransactions) {
        procContext().logger().info("aaa onSnapshotCompleted, commitTransactions=" + commitTransactions);
        assert waitingForPhase2 : "not waiting for phase 2";
        waitingForPhase2 = false;
        if (commitTransactions) {
            for (TXN txn : pendingTransactions.values()) {
                try {
                    txn.commit();
                    txn.release();
                } catch (Exception e) {
                    throw sneakyThrow(e);
                }
            }
            pendingTransactions.clear();
        }
        return true;
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        @SuppressWarnings("unchecked")
        TXN_ID txnId = ((BroadcastKey<TXN_ID>) key).key();
        if (txnId.index() % procContext().totalParallelism() == procContext().globalProcessorIndex()) {
            recoverAndCommitFn().accept(txnId);
        }
    }

    @Override
    public void close() throws Exception {
        procContext().logger().info("aaa close");
        if (activeTransaction != null) {
            activeTransaction.release();
            activeTransaction = null;
        }
        for (TXN txn : pendingTransactions.values()) {
            txn.release();
        }
        pendingTransactions.clear();
    }
}
