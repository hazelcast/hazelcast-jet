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
 * A source stage in a distributed computation {@link Pipeline pipeline} that
 * will observe an unbounded amount of data (i.e., an event stream), before
 * timestamps are specified.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface StreamSourceStage<T> {

    /**
     * Declares that the source will not emit items with timestamp. You can add
     * them later using {@link
     * GeneralStage#addTimestamps(DistributedToLongFunction, long)
     * addTimestamps}, but the behavior is different - see the note there.
     */
    StreamStage<T> withoutTimestamps();

    /**
     * Declares that the source will use ingestion time. {@code
     * System.currentTimeMillis()} time will be assigned to each event at the
     * processing time in source.
     */
    StreamStage<T> withIngestionTimestamps();

    /**
     * Declares that the stream will use source's default timestamp. This
     * typically is the message timestamp of the external system.
     * <p>
     * If there's no notion of default timestamp in the source, the job will
     * fail at runtime when the first event is processed; in that case you have
     * to use {@link #withTimestamps} or {@link #withIngestionTimestamps}, for
     * example.
     *
     * @param allowedLag the allowed lag behind the top observed timestamp
     */
    StreamStage<T> withDefaultTimestamps(long allowedLag);

    /**
     * Declares that the source will extract timestamps from the stream items.
     *
     * @param timestampFn a function that returns the timestamp for each item,
     *                    typically in milliseconds
     * @param allowedLag the allowed lag behind the top observed timestamp.
     *                   Time unit is the same as the unit used by {@code
     *                   timestampFn}
     */
    StreamStage<T> withTimestamps(@Nonnull DistributedToLongFunction<? super T> timestampFn, long allowedLag);
}
