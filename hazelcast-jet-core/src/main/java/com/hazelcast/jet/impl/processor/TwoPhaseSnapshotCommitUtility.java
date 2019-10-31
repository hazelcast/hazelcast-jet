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
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class TwoPhaseSnapshotCommitUtility<TXN_ID extends TransactionId,
        TXN extends TransactionalResource<TXN_ID>> {

    private final Outbox outbox;
    private final Context procContext;
    private final ProcessingGuarantee externalGuarantee;
    private final Function<Boolean, TXN> createTxnFn;
    private final Consumer<TXN_ID> recoverAndCommitFn;
    private final ConsumerEx<Integer> recoverAndAbortFn;

    public TwoPhaseSnapshotCommitUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull FunctionEx<Boolean, TXN> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull ConsumerEx<Integer> recoverAndAbortFn
    ) {
        if (externalGuarantee != procContext.processingGuarantee()
                && externalGuarantee != ProcessingGuarantee.AT_LEAST_ONCE
                && procContext.processingGuarantee() != ProcessingGuarantee.EXACTLY_ONCE) {
            throw new IllegalArgumentException("unsupported combination, job guarantee cannot by lower than external "
                    + "guarantee. Job guarantee: " + procContext.processingGuarantee() + ", external guarantee: "
                    + externalGuarantee);
        }
        this.outbox = outbox;
        this.procContext = procContext;
        this.externalGuarantee = externalGuarantee;
        this.createTxnFn = createTxnFn;
        this.recoverAndCommitFn = recoverAndCommitFn;
        this.recoverAndAbortFn = recoverAndAbortFn;
    }

    public ProcessingGuarantee externalGuarantee() {
        return externalGuarantee;
    }

    protected Outbox getOutbox() {
        return outbox;
    }

    protected Context procContext() {
        return procContext;
    }

    protected Function<Boolean, TXN> createTxnFn() {
        return createTxnFn;
    }

    protected Consumer<TXN_ID> recoverAndCommitFn() {
        return recoverAndCommitFn;
    }

    protected ConsumerEx<Integer> recoverAndAbortFn() {
        return recoverAndAbortFn;
    }

    /**
     * Returns the active transaction that can be used to store an item or
     * query the source. It's null in case when transaction is not available
     * now. In that case the processor should back off and retry later.
     */
    @Nullable
    public abstract TXN activeTransaction();

    /**
     * For sinks and inner vertices, call from {@link Processor#complete()}.
     * For batch sources call after the source emitted everything. Never call
     * it for streaming sources.
     */
    public abstract void afterCompleted();

    /**
     * Delegate handling of {@link Processor#saveToSnapshot()} to this method.
     *
     * @return a value to return from {@code saveToSnapshot()}
     */
    public abstract boolean saveToSnapshot();

    /**
     * Delegate handling of {@link Processor#onSnapshotCompleted(boolean)} to
     * this method.
     *
     * @param commitTransactions value passed to {@code onSnapshotCompleted}
     * @return value to return from {@code onSnapshotCompleted}
     */
    public abstract boolean onSnapshotCompleted(boolean commitTransactions);

    /**
     * Delegate handling of {@link Processor#restoreFromSnapshot(Inbox)} to
     * this method. If you save custom items to snapshot besides those saved by
     * {@link #saveToSnapshot()} of this utility, use {@link
     * #restoreFromSnapshot(Entry)} to pass only entries not handled by your
     * processor.
     *
     * @param inbox the inbox passed to {@code restoreFromSnapshot()}
     */
    @SuppressWarnings("unchecked")
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.poll()) != null; ) {
            restoreFromSnapshot((Entry<BroadcastKey<TXN_ID>, Boolean>) item);
        }
    }

    /**
     * Delegate handling of {@link Processor#restoreFromSnapshot(Inbox)} to
     * this method.
     * <p>
     * See also {@link #restoreFromSnapshot(Inbox)}.
     *
     * @param snapshotEntry an entry from the inbox
     */
    public abstract void restoreFromSnapshot(@Nonnull Entry<BroadcastKey<TXN_ID>, Boolean> snapshotEntry);

    /**
     * Delegate handling of {@link Processor#finishSnapshotRestore()} to this
     * method.
     *
     * @return value to return from {@code finishSnapshotRestore()}
     */
    public abstract boolean finishSnapshotRestore();

    /**
     * Call from {@link Processor#close()}.
     *
     * The implementation must not commit or rollback any pending transactions
     * - the job might have failed between after snapshot phase 1 and 2. The
     * pending transactions might be recovered after the restart.
     */
    public abstract void close() throws Exception;

    /**
     * A handle for a transactional resource.
     * <p>
     * This is the process if the transaction was created with a {@code true}
     * parameter to the {@link #createTxnFn()} (with transactions):
     * <ol>
     *     <li>{@link #beginTransaction()}
     *     <li>{@link #prepare()} - after this the transaction will no longer
     *     returned from {@link #activeTransaction()}, i.e. no more data will
     *     be written to it
     *     <li>{@link #commit()}
     *     <li>if the utility recycles transaction, the process can go to (1)
     *     <li>{@link #release()}
     * </ol>
     *
     * If the transaction was created with a {@code false} parameter to the
     * {@link #createTxnFn()} (resource without transactions), the process is
     * as follows:
     * <ol>
     *     <li>{@link #flush()} - zero or multiple times. Called to ensure
     *     at-least-once processing guarantee.
     *     <li>{@link #release()}
     * </ol>
     *
     * @param <TXN_ID> type of transaction identifier. Must be serializable, will
     *                be saved to state snapshot
     */
    public interface TransactionalResource<TXN_ID> {

        /**
         * Returns the ID of this transaction.
         */
        TXN_ID id();

        /**
         * Begins the transaction. The implementation needs to ensure that in
         * case the transaction ID was used before, the work from before is
         * rolled back before the new transaction is begun.
         *
         * @throws UnsupportedOperationException if the transaction was created
         * with {@code false} parameter to the {@link #createTxnFn()},
         * otherwise it will be called immediately after creation.
         */
        void beginTransaction() throws Exception;

        /**
         * Flushes all previous writes to a durable storage. This method can be
         * called multiple times.
         */
        default void flush() throws Exception {
        }

        /**
         * Flushes all previous writes to a durable storage and prepares for
         * commit. To achieve correctness, the transaction must be eventually
         * able to successfully commit.
         * <p>
         * After this call, the transaction will never again be returned from
         * {@link #activeTransaction()} until it's committed.
         */
        default void prepare() throws Exception {
        }

        /**
         * Flushes the outstanding items and makes them visible to others.
         */
        default void commit() throws Exception {
            throw new UnsupportedOperationException();
        }

        /**
         * Release the resources associated with the transaction. Must not
         * commit or rollback it, the transaction can be later recovered from
         * the durable storage and continued.
         */
        default void release() throws Exception {
        }
    }

    public interface TransactionId {

        /**
         * Returns the index of the processor that will handle this transaction
         * ID. Used when restoring transaction to determine which processor
         * owns which transactions.
         * <p>
         * After restoring the ID from the snapshot the index might be out of
         * range (greater or equal to the current total parallelism).
         */
        int index();
    }
}
