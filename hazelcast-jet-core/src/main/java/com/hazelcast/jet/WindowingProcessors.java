/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.processor.InsertPunctuationP;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.TimestampKind.EVENT_TIMESTAMP;
import static com.hazelcast.jet.TimestampKind.FRAME_TIMESTAMP;
import static com.hazelcast.jet.function.DistributedFunction.identity;

/**
 * Contains factory methods for processors dealing with windowing
 * operations.
 *
 * <h1>Two-stage aggregation</h1>
 *
 * This setup first aggregates events on the local member and then sends
 * just the per-key aggregation state over the distributed edge. Compared
 * to the single-stage setup this can dramatically reduce network traffic,
 * but it will have to keep track of all keys on each cluster member. The
 * complete DAG should look like the following:
 *
 * <pre>
 *             -------------------------
 *            | source with punctuation |
 *             -------------------------
 *                        |
 *                        | (local partitioned edge)
 *                        V
 *               ---------------------
 *              | slidingWindowStage1 |
 *               ---------------------
 *                        |
 *                        | (distributed partitioned edge)
 *                        V
 *               ---------------------
 *              | slidingWindowStage2 |
 *               ---------------------
 *                        |
 *                        | (local edge)
 *                        V
 *             ----------------------------
 *            | sink or further processing |
 *             ----------------------------
 * </pre>
 *
 * To get consistent results, the same {@link WindowDefinition} and {@link
 * AggregateOperation} must be used for both stages.
 *
 * <h1>Single-stage aggregation</h1>
 *
 * In this setup there is only one processing stage, so its input must be
 * properly partitioned and distributed. If the source is already
 * partitioned by the grouping key, this setup is the best choice. Another
 * reason may be memory constraints because with this setup, each member
 * keeps track only of the keys belonging to its own partitions. This is the
 * expected DAG:
 * <pre>
 *              -------------------------
 *             | source with punctuation |
 *              -------------------------
 *                         |
 *                         | (partitioned edge, distributed as needed)
 *                         V
 *                 --------------------
                  |    single-stage    |
 *                |  window aggregator |
 *                 --------------------
 *                         |
 *                         | (local edge)
 *                         V
 *           ----------------------------
 *          | sink or further processing |
 *           ----------------------------
 * </pre>
 */
public final class WindowingProcessors {

    private WindowingProcessors() {
    }

    /**
     * A processor that inserts {@link com.hazelcast.jet.Punctuation
     * punctuation} into a data (sub)stream. The value of the punctuation is
     * determined by a separate policy object of type {@link
     * PunctuationPolicy}.
     *
     * @param <T> the type of stream item
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> insertPunctuation(
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<PunctuationPolicy> newPuncPolicyF
    ) {
        return () -> new InsertPunctuationP<>(getTimestampF, newPuncPolicyF.get());
    }

    /**
     * A processor that performs a general group-by-key-and-window operation and
     * applies the provided aggregate operation on groups.
     *
     * @param getKeyF function that extracts the grouping key from the input item
     * @param getTimestampF function that extracts the timestamp from the input item
     * @param timestampKind the kind of timestamp extracted by {@code getTimestampF}: either the
     *                      event timestamp or the frame timestamp
     * @param windowDef definition of the window to compute
     * @param aggregateOperation aggregate operation to perform on each group in a window
     * @param <T> type of stream item
     * @param <K> type of grouping key
     * @param <A> type of the aggregate operation's accumulator
     * @param <R> type of the aggregated result
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateByKeyAndWindow(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return () -> new SlidingWindowP<T, A, R>(
                getKeyF,
                timestampKind == EVENT_TIMESTAMP
                        ? item -> windowDef.higherFrameTs(getTimestampF.applyAsLong(item))
                        : getTimestampF,
                windowDef,
                aggregateOperation);
    }

    /**
     * A single-stage processor that aggregates events into a sliding window
     * (see the {@link WindowingProcessors class Javadoc} for an overview). The
     * processor groups items by the grouping key (as obtained from the given
     * key extractor) and by <em>frame</em>, which is a range of timestamps
     * equal to the sliding step. When it receives a punctuation, it combines
     * consecutive frames into sliding windows of the requested size. To
     * calculate the finalized window result it applies the finishing function
     * to the combined frames. All windows that end before the punctuation are
     * computed.
     * <p>
     * The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per window. The
     * item's timestamp is the upper exclusive bound of the timestamp range
     * covered by the window.
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> slidingWindow(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getEventTimestampF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return WindowingProcessors.<T, K, A, R>aggregateByKeyAndWindow(
                getKeyF,
                getEventTimestampF,
                EVENT_TIMESTAMP,
                windowDef,
                aggregateOperation
        );
    }

    /**
     * The first-stage processor in a two-stage sliding window aggregation
     * setup (see the {@link WindowingProcessors class Javadoc} for an
     * overview). The processor groups items by the grouping key (as obtained
     * from the given key extractor) and by <em>frame</em>, which is a range
     * of timestamps equal to the sliding step. The frame is identified by its
     * timestamp, which is the upper exclusive bound of its timestamp range.
     * {@link WindowDefinition#higherFrameTs(long)} maps an item's timestamp to
     * the timestamp of the frame it belongs to.
     * <p>
     * When the processor receives a punctuation with a given {@code puncVal},
     * it emits the current accumulated state of all frames with {@code
     * timestamp <= puncVal} and deletes these frames from its storage.
     * The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per frame.
     *
     * @param <T> input item type
     * @param <K> type of key returned from {@code getKeyF}
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorF()}
     */
    @Nonnull
    public static <T, K, A> DistributedSupplier<Processor> slidingWindowStage1(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getEventTimestampF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, ?> aggregateOperation
    ) {
        // use a single-frame window in this stage; the downstream processor
        // combines the frames into a window with the user-requested size
        WindowDefinition tumblingByFrame = windowDef.toTumblingByFrame();
        return WindowingProcessors.<T, K, A, A>aggregateByKeyAndWindow(
                getKeyF,
                getEventTimestampF,
                EVENT_TIMESTAMP,
                tumblingByFrame,
                aggregateOperation.withFinish(identity())
        );
    }

    /**
     * Constructs sliding windows by combining their constituent frames
     * received from several upstream instances of {@link
     * #slidingWindowStage1(DistributedFunction, DistributedToLongFunction,
     * WindowDefinition, AggregateOperation)}. After combining it applies the
     * {@code windowOperation}'s finishing function to compute the emitted
     * result.
     * <p>
     * The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, R>}. The item's timestamp is the upper exclusive
     * bound of the timestamp range covered by the window.
     *
     * @param <A> type of the accumulator
     * @param <R> type of the finishing function's result
     */
    @Nonnull
    public static <K, A, R> DistributedSupplier<Processor> slidingWindowStage2(
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<?, A, R> aggregateOperation
    ) {
        return aggregateByKeyAndWindow(
                TimestampedEntry<K, A>::getKey,
                TimestampedEntry::getTimestamp,
                FRAME_TIMESTAMP,
                windowDef,
                withFrameCombining(aggregateOperation)
        );
    }

    /**
     * Aggregates events into session windows. Events and windows under
     * different grouping keys are treated independently.
     * <p>
     * The functioning of this processor is easiest to explain in terms of
     * the <em>event interval</em>: the range {@code [timestamp, timestamp +
     * sessionTimeout]}. Initially an event causes a new session window to be
     * created, covering exactly the event interval. A following event under
     * the same key belongs to this window iff its interval overlaps it. The
     * window is extended to cover the entire interval of the new event. The
     * event may happen to belong to two existing windows if its interval
     * bridges the gap between them; in that case they are combined into one.
     *
     * @param sessionTimeout    maximum gap between consecutive events in the same session window
     * @param getTimestampF function to extract the timestamp from the item
     * @param getKeyF       function to extract the grouping key from the item
     * @param aggregateOperation   contains aggregation logic
     *
     * @param <T> type of the stream event
     * @param <K> type of the item's grouping key
     * @param <A> type of the container of the accumulated value
     * @param <R> type of the session window's result value
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> sessionWindow(
            long sessionTimeout,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedCollector<? super T, A, R> aggregateOperation
    ) {
        return () -> new SessionWindowP<>(sessionTimeout, getTimestampF, getKeyF, aggregateOperation);
    }

    private static <A, R> AggregateOperation<TimestampedEntry<?, A>, A, R> withFrameCombining(
            AggregateOperation<?, A, R> aggrOp
    ) {
        return aggrOp.withAccumulate(
                (A acc, TimestampedEntry<?, A> tsEntry) ->
                        aggrOp.combineAccumulatorsF().accept(acc, tsEntry.getValue()));
    }
}
