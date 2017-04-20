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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for processors dealing with windowing operations.
 */
public final class WindowingProcessors {

    private WindowingProcessors() {
    }

    /**
     * A processor that inserts punctuation into a data stream. A punctuation
     * item contains a {@code puncSeq} value with this meaning: "there will be
     * no more items in this stream with {@code eventSeq < puncSeq}". The value
     * of punctuation is determined by a separate policy object of type
     * {@link PunctuationPolicy}.
     *
     * @param <T> the type of stream item
     */
    public static <T> Distributed.Supplier<InsertPunctuationP<T>> insertPunctuation(
            @Nonnull Distributed.ToLongFunction<T> extractEventSeqF,
            @Nonnull Distributed.Supplier<PunctuationPolicy> newPuncPolicyF
    ) {
        return () -> new InsertPunctuationP<>(extractEventSeqF, newPuncPolicyF.get());
    }

    /**
     * Groups items into frames. A frame is identified by its {@code long frameSeq};
     * the {@code extractFrameSeqF} function determines this number for each item.
     * Within a frame items are further classified by a grouping key determined by
     * the {@code extractKeyF} function. When the processor receives a punctuation
     * with a given {@code puncSeq}, it emits the current state of all frames with
     * {@code frameSeq <= puncSeq} and deletes these frames from its storage.
     *
     * @param <T> item type
     * @param <K> type of key returned from {@code extractKeyF}
     * @param <F> type of frame returned from {@code sc.supplier()}
     */
    public static <T, K, F> Distributed.Supplier<GroupByFrameP<T, K, F>> groupByFrame(
            Distributed.Function<? super T, K> extractKeyF,
            Distributed.ToLongFunction<? super T> extractTimestampF,
            WindowDefinition windowDef,
            WindowOperation<T, F, ?> collector
    ) {
        return () -> new GroupByFrameP<>(extractKeyF, extractTimestampF, windowDef, collector);
    }

    /**
     * Convenience for {@link #groupByFrame(
     * Distributed.Function, Distributed.ToLongFunction, WindowDefinition, WindowOperation)
     * groupByFrame(extractKeyF, extractTimeStampF, frameLength, frameOffset, collector)}
     * which doesn't group by key.
     */
    public static <T, F> Distributed.Supplier<GroupByFrameP<T, String, F>> groupByFrame(
            Distributed.ToLongFunction<? super T> extractTimestampF,
            WindowDefinition windowDef,
            WindowOperation<T, F, ?> collector
    ) {
        return groupByFrame(t -> "global", extractTimestampF, windowDef, collector);
    }

    /**
     * Combines frames received from several upstream instances of {@link
     * GroupByFrameP} into finalized frames. Combines finalized frames into
     * sliding windows. Applies the finisher function to produce its emitted
     * output.
     *
     * @param <K> type of the grouping key
     * @param <F> type of the frame
     * @param <R> type of the result derived from a frame
     * @param emitPunctuation If {@code true}, punctuation will be emitted when window
     *                        is closed. Enable, if downstream vertex needs to know,
     *                        that it has received all keys for a particular window.
     */
    public static <K, F, R> Distributed.Supplier<SlidingWindowP<K, F, R>> slidingWindow(
            WindowDefinition windowDef, WindowOperation<K, F, R> windowOperation, boolean emitPunctuation) {
        return () -> new SlidingWindowP<>(windowDef, windowOperation, emitPunctuation);
    }

    /**
     * Aggregates events into session windows. Events and windows under
     * different grouping keys behave independenly.
     * <p>
     * The functioning of this processor is easiest to explain in terms of
     * the <em>event interval</em>: the range {@code [eventSeq, eventSeq + maxSeqGap]}.
     * Initially an event causes a new session window to be created, covering
     * exactly the event interval. A following event under the same key belongs
     * to this window iff its interval overlaps it. The window is extended to
     * cover the entire interval of the new event. The event may happen to
     * belong to two existing windows if its interval bridges the gap between
     * them; in that case they are combined into one.
     *
     * @param maxSeqGap        maximum gap between consecutive events in the same session window
     * @param extractEventSeqF function to extract the event seq from the event item
     * @param extractKeyF      function to extract the grouping key from the event iem
     * @param collector        contains aggregation logic
     *
     * @param <T> type of stream event
     * @param <K> type of event's grouping key
     * @param <A> type of the container of accumulated value
     * @param <R> type of the result value for a session window
     */
    public static <T, K, A, R> Distributed.Supplier<SessionWindowP<T, K, A, R>> sessionWindow(
            long maxSeqGap,
            Distributed.ToLongFunction<? super T> extractEventSeqF,
            Distributed.Function<? super T, K> extractKeyF,
            DistributedCollector<? super T, A, R> collector
    ) {
        return () -> new SessionWindowP<>(maxSeqGap, extractEventSeqF, extractKeyF, collector);
    }
}
