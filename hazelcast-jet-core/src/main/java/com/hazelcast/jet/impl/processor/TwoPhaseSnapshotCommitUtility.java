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
import com.hazelcast.jet.core.AbstractProcessor;
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

public abstract class TwoPhaseSnapshotCommitUtility<TXN_ID extends TransactionId,
        TXN extends TransactionalResource<TXN_ID>> {

    private final Outbox outbox;
    private final Context procContext;
    private final ProcessingGuarantee externalGuarantee;
    private final FunctionEx<TXN_ID, TXN> createTxnFn;
    private final Consumer<TXN_ID> recoverAndCommitFn;
    private final ConsumerEx<Integer> recoverAndAbortFn;

    private boolean snapshotInProgress;

    /**
     *
     * @param outbox
     * @param procContext
     * @param externalGuarantee
     * @param createTxnFn creates a {@link TransactionalResource} based on a
     *      transaction id. The implementation needs to ensure that in case
     *      when the transaction ID was used before, the work from the previous
     *      use is rolled back.
     * @param recoverAndCommitFn
     * @param recoverAndAbortFn
     */
    public TwoPhaseSnapshotCommitUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull FunctionEx<TXN_ID, TXN> createTxnFn,
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

    protected FunctionEx<TXN_ID, TXN> createTxnFn() {
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
     * Returns if there was a phase-1 of a snapshot and we're waiting for
     * phase-2.
     */
    public boolean isSnapshotInProgress() {
        return snapshotInProgress;
    }

    public void setSnapshotInProgress(boolean snapshotInProgress) {
        this.snapshotInProgress = snapshotInProgress;
    }

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
     * #restoreFromSnapshot(Object, Object)} to pass only entries not handled
     * by your processor.
     *
     * @param inbox the inbox passed to {@code Processor.restoreFromSnapshot()}
     */
    @SuppressWarnings("unchecked")
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.poll()) != null; ) {
            Entry<BroadcastKey<TXN_ID>, Boolean> castedItem = (Entry<BroadcastKey<TXN_ID>, Boolean>) item;
            restoreFromSnapshot(castedItem.getKey(), castedItem.getValue());
        }
    }

    /**
     * Delegate handling of {@link
     * AbstractProcessor#restoreFromSnapshot(Object, Object)} to this method.
     * <p>
     * See also {@link #restoreFromSnapshot(Inbox)}.
     *
     * @param key a key from the snapshot
     * @param value a value from the snapshot
     */
    public abstract void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value);

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
     * The methods are called depending on the external guarantee:<ul>
     *
     * <li>EXACTLY_ONCE
     *
     * <ol>
     *     <li>{@link #begin()} - called before the transaction is first
     *         returned from {@link #activeTransaction()}
     *     <li>{@link #flush()}
     *     <li>{@link #endAndPrepare()} - after this the transaction will no
     *         longer be returned from {@link #activeTransaction()}, i.e. no
     *         more data will be written to it. Called in the 1st snapshot
     *         phase
     *     <li>{@link #commit()} - called in the 2nd snapshot phase
     *     <li>if the utility recycles transaction, the process can go to (1)
     *     <li>{@link #release()}
     * </ol>
     *
     * <li>AT_LEAST_ONCE

     * <ol>
     *     <li>{@link #flush()} - ensure all writes are stored in the external
     *         system. Called in the 1st snapshot phase
     *     <li>{@link #commit()} - acknowledge the consumption of items
     *         when they don't need to be delivered again. Called in the 2nd
     *         snapshot phase
     *     <li>if the utility recycles transaction, the process can go to (1)
     *     <li>{@link #release()}
     * </ol>
     *
     * <li>NONE
     *
     * <ol>
     *     <li>{@link #release()}
     * </ol>
     *
     * </ul>
     *
     * @param <TXN_ID> type of transaction identifier. Must be serializable, will
     *                be saved to state snapshot
     */
    public interface TransactionalResource<TXN_ID> {

        /**
         * Returns the ID of this transaction, it should be the ID passed to
         * the {@link #createTxnFn()}.
         */
        TXN_ID id();

        /**
         * Begins the transaction. The method will be called before the
         * transaction is returned from {@link #activeTransaction()} for the
         * first time after creation or after {@link #commit()}.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         *
         * @throws UnsupportedOperationException if the transaction was created
         * with {@code null} id passed to the {@link #createTxnFn()}
         */
        default void begin() throws Exception {
            throw new UnsupportedOperationException("Resource without transaction support");
        }

        /**
         * Flushes all previous writes to the external system and ensures all
         * pending items are emitted to the downstream.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         *
         * @return if all was flushed and emitted. If the method returns false,
         *      it will be called again before any other method is called.
         */
        default boolean flush() throws Exception {
            return true;
        }

        /**
         * Prepares for a commit. To achieve correctness, the transaction must
         * be able to eventually commit after this call, writes must be durably
         * stored in the external system.
         * <p>
         * After this call, the transaction will never again be returned from
         * {@link #activeTransaction()} until it's committed.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         */
        default void endAndPrepare() throws Exception {
        }

        /**
         * Makes the changes visible to others and acknowledges consumed items.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         */
        default void commit() throws Exception {
            throw new UnsupportedOperationException();
        }

        /**
         * Release the resources associated with the transaction. Must not
         * commit or rollback it, the transaction can be later recovered from
         * the durable storage and continued.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         */
        default void release() throws Exception {
        }
    }

    public interface TransactionId {

        /**
         * Returns the index of the processor that will handle this transaction
         * ID. Used when restoring transaction IDs to determine which processor
         * owns which transactions.
         * <p>
         * After restoring the ID from the snapshot the index might be out of
         * range (greater or equal to the current total parallelism).
         */
        int index();
    }
}
