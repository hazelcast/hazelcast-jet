/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.impl.WatermarkSourceUtilImpl;
import com.hazelcast.jet.impl.WatermarkSourceUtilNoWatermarksImpl;
import com.hazelcast.jet.pipeline.Sources;

import javax.annotation.Nonnull;

/**
 * A utility to help emitting {@link Watermark}s from a source which reads
 * events from multiple external partitions.
 *
 * <h3>The problems</h3>
 *
 * <h4>1. Reading partition by partition</h4>
 *
 * Upon restart it can happen that partition <em>partition1</em> has one
 * very recent event and <em>partition2</em> has an old one. If we poll
 * <em>partition1</em> first and emit its recent event, it will advance the
 * watermark. When we poll <em>partition2</em> later on, its event will be
 * behind the watermark and can be dropped as late. This utility tracks the
 * event timestamps for each partition individually and allows the
 * processor to emit the watermark that is correct with respect to all the
 * partitions.
 *
 * <h4>2. Some partition having no data</h4>
 *
 * It can happen that some partition does not have any events at all while
 * others do or the processor doesn't get any external partitions assigned
 * to it. If we simply wait for the timestamps in all partitions to advance
 * to some point, we won't be emitting any watermarks. This utility
 * supports the <em>idle timeout</em>: if there's no new data from a
 * partition after the timeout elapses, it will be marked as <em>idle</em>,
 * allowing the processor's watermark to advance as if that partition didn't
 * exist. If all partitions are idle or there are no partitions, the
 * processor will emit a special <em>idle message</em> and the downstream
 * will exclude this processor from watermark coalescing.
 *
 * <h3>Usage</h3>
 *
 * The API is designed to be used as a flat-mapping step in the {@link
 * Traverser} that holds the output data. Your source can follow this
 * pattern:
 *
 * <pre>{@code
 * public boolean complete() {
 *     if (traverser == null) {
 *         List<Record> records = poll();
 *         if (records.isEmpty()) {
 *             traverser = wmSourceUtil.handleNoEvent();
 *         } else {
 *             traverser = traverserIterable(records)
 *                 .flatMap(event -> wmSourceUtil.handleEvent(
 *                      event, event.getPartition()));
 *         }
 *         traverser = traverser.onFirstNull(() -> traverser = null);
 *     }
 *     emitFromTraverser(traverser, event -> {
 *         if (!(event instanceof Watermark)) {
 *             // store your offset after event was emitted
 *             offsetsMap.put(event.getPartition(), event.getOffset());
 *         }
 *     });
 *     return false;
 * }
 * }</pre>
 *
 * Other methods:
 * <ul><li>
 *     Call {@link #increasePartitionCount} to set your partition count
 *     initially or whenever the count increases.
 * <li>
 *     If you support state snapshots, save the value returned by {@link
 *     #getWatermark} for all partitions to the snapshot. When restoring the
 *     state, call {@link #restoreWatermark}.
 *     <br>
 *     You should save the value under your external partition key so that the
 *     watermark value can be restored to correct processor instance. The key
 *     should also be wrapped using {@link BroadcastKey#broadcastKey
 *     broadcastKey()}, because the external partitions don't match Hazelcast
 *     partitions. This way, all processor instances will see all keys and they
 *     can restore partition they handle and ignore others.
 * </ul>
 *
 * @param <T> event type
 */
public interface WatermarkSourceUtil<T> {

    /**
     * Value to use as the {@code nativeEventTime} argument when calling
     * {@link #handleEvent(Object, int, long)} where there's no native event
     * time to supply.
     */
    long NO_NATIVE_TIME = Long.MIN_VALUE;

    /**
     * Creates a new instance of the utility, see {@linkplain
     * WatermarkSourceUtil class javadoc} for more information.
     * <p>
     * The partition count is initially set to 0, call {@link
     * #increasePartitionCount} to set it.
     *
     * @param eventTimePolicy event time policy as passed in {@link
     *                        Sources#streamFromProcessorWithWatermarks}
     */
    static <T> WatermarkSourceUtil<T> create(EventTimePolicy<T> eventTimePolicy) {
        if (eventTimePolicy.watermarkThrottlingFrameSize() == 0) {
            return new WatermarkSourceUtilNoWatermarksImpl<>();
        } else {
            return new WatermarkSourceUtilImpl<>(eventTimePolicy);
        }
    }

    /**
     * Flat-maps the given {@code event} by (possibly) prepending it with a
     * watermark. Designed to use when emitting from traverser:
     * <pre>{@code
     *     Traverser t = traverserIterable(...)
     *         .flatMap(event -> watermarkSourceUtil.handleEvent(
     *                 event, event.getPartition(), nativeEventTime));
     * }</pre>
     *
     * @param event           the event
     * @param partitionIndex  the source partition index the event came from
     * @param nativeEventTime native event time in case no {@code timestampFn} was supplied or
     *                        {@link #NO_NATIVE_TIME} if the event has no native timestamp
     */
    @Nonnull
    Traverser<Object> handleEvent(T event, int partitionIndex, long nativeEventTime);

    /**
     * Call this method when there is no event coming. It returns a traverser
     * with 0 or 1 object (the watermark). If you need just the Watermark, call
     * {@code next()} on the result.
     */
    @Nonnull
    Traverser<Object> handleNoEvent();

    /**
     * Changes the partition count. The new partition count must be higher or
     * equal to the current count.
     * <p>
     * You can call this method at any moment. Added partitions will be
     * considered <em>active</em> initially.
     *
     * @param newPartitionCount partition count, must be higher than the
     *                          current count
     */
    void increasePartitionCount(int newPartitionCount);

    /**
     * Watermark value to be saved to state snapshot for the given source
     * partition index. The returned value should be {@link
     * #restoreWatermark(int, long) restored} to a processor handling the same
     * partition after restart.
     * <p>
     * Method is meant to be used from {@link Processor#saveToSnapshot()}.
     *
     * @param partitionIndex 0-based source partition index.
     * @return A value to save to state snapshot
     */
    long getWatermark(int partitionIndex);

    /**
     * Restore watermark value from state snapshot.
     * <p>
     * Method is meant to be used from {@link
     * Processor#restoreFromSnapshot(Inbox)}.
     * <p>
     * See {@link #getWatermark(int)}.
     *
     * @param partitionIndex 0-based source partition index.
     * @param wm Watermark value to restore
     */
    void restoreWatermark(int partitionIndex, long wm);
}

