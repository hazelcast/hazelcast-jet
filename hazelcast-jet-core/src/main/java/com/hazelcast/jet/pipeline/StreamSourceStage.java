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
 * TODO [viliam]
 * @param <T>
 */
public interface StreamSourceStage<T> {

    /**
     * Declares that the source will not emit items with timestamp.
     * You can add them later using {@link GeneralStage#addTimestamps}, however,
     * source partitions won't be coalesced correctly...
     *
     * TODO [viliam]
     */
    StreamStage<T> withoutTimestamps();

    /**
     * Declares that the source will use ingestion time.
     *
     * TODO [viliam]
     */
    StreamStage<T> withIngestionTimestamps();

    /**
     * Declares that the stream will use source's default timestamp. Throws,
     * if the source doesn't have the concept of default timestamps (??).
     *
     * TODO [viliam]
     */
    StreamStage<T> withDefaultTimestamps(long allowedLag);

    /**
     * Declares that the source will extract timestamps from the stream items.
     *
     * TODO [viliam]
     */
    StreamStage<T> withTimestamps(@Nonnull DistributedToLongFunction<? super T> timestampFn, long allowedLag);
}
