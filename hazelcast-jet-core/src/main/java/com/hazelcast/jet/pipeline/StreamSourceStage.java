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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;

/**
 * A source stage in a distributed computation {@link Pipeline pipeline}
 * that will observe an unbounded amount of data (i.e., an event stream).
 * Timestamp handling, a prerequisite for attaching data processing stages,
 * is not yet defined in this step. Call one of the methods on this
 * instance to declare whether and how the data source will assign
 * timestamps to events.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface StreamSourceStage<T> {

    /**
     * Declares that the source will not assign any timestamp to the events it
     * emits. You can add them later using {@link
     * GeneralStage#addTimestamps(DistributedToLongFunction, long) addTimestamps},
     * but the behavior is different &mdash; see the note there.
     */
    StreamStage<T> withoutTimestamps();

    /**
     * Declares that the source will assign the time of ingestion as the event
     * timestamp. It will call {@code System.currentTimeMillis()} at the moment
     * it observes an event from the data source and assign it as the event
     * timestamp.
     * <p>
     * <strong>Note:</strong> when snapshotting is enabled to achieve fault
     * tolerance, after a restart Jet replays all the events that were already
     * processed since the last snapshot. These events will now get different
     * timestamps. If you want your job to be fault-tolerant, the events in the
     * stream must have a stable timestamp associated with them. The source may
     * natively provide such timestamps (the {@link #withDefaultTimestamps(long)}
     * option). If that is not appropriate, the event should carry its own
     * timestap as a part of its data and you can use {@link
     * #withTimestamps(DistributedToLongFunction, long)
     * withTimestamps(timestampFn, allowedLag} to extract them.
     * <p>
     * <strong>Note 2:</strong> if the system time goes back (such as when
     * adjusting the system time), newer events will get older timestamps and
     * might be dropped as late, because the allowed lag is 0.
     */
    default StreamStage<T> withIngestionTimestamps() {
        return withTimestamps(o -> System.currentTimeMillis(), 0);
    }

    /**
     * Declares that the stream will use the source's default timestamps. This
     * is typically the message timestamp that the external system assigns as
     * event's metadata.
     * <p>
     * If there's no notion of a default timestamp in the source, the job will
     * fail at runtime when it tries to processes the first event.
     *
     * @param allowedLag the allowed lag of a given event's timestamp behind the top
     *                   timestamp value observed so far.
     */
    StreamStage<T> withDefaultTimestamps(long allowedLag);

    /**
     * Declares that the source will extract timestamps from the stream items.
     *
     * @param timestampFn a function that returns the timestamp for each item, typically in
     *                    milliseconds
     * @param allowedLag the allowed lag of a given event's timestamp behind the top
     *                   timestamp value observed so far. The time unit is
     *                   the same as the unit used by {@code timestampFn}
     */
    StreamStage<T> withTimestamps(@Nonnull DistributedToLongFunction<? super T> timestampFn, long allowedLag);
}
