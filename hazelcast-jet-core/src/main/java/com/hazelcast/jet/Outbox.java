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

import javax.annotation.Nonnull;

/**
 * Data sink for a {@link Processor}. The outbox consists of individual
 * output buckets, one per outbound edge of the vertex represented by
 * the associated processor. The processor must deliver its output items,
 * separated by destination edge, into the outbox by calling
 * {@link #add(int, Object)} or {@link #add(Object)}.
 * <p>
 * In the case of a processor declared as <em>cooperative</em>, the
 * execution engine will not try to flush the outbox into downstream
 * queues until the processing method returns. Therefore the processor
 * must check {@link #isFull()} or {@link #isFull(int)} regularly and
 * refrain from outputting more data when it returns true.
 * <p>
 * A non-cooperative processor's outbox will have auto-flushing behavior
 * and each item will be immediatelly flushed to the edge, blocking as
 * needed until success.
 */
public interface Outbox {

    /**
     * Returns the number of buckets in this outbox.
     */
    int bucketCount();

    /**
     * Adds the supplied item to all the output buckets.
     * <p>
     * Must not be called when the outbox is full ({@link #isFull()}
     * would return {@code true}).
     *
     * @throws IndexOutOfBoundsException if the outbox is full
     */
    default void add(@Nonnull Object item) {
        add(-1, item);
    }

    /**
     * Adds the supplied item to the output bucket with the supplied ordinal.
     * If {@code ordinal == -1}, adds the supplied item to all buckets
     * (behaves the same as {@link #add(Object)}).
     * <p>
     * Must not be called when the outbox is full ({@link #isFull(int)
     * isFull(ordinal)} would return {@code true}).
     *
     * @throws IndexOutOfBoundsException if the bucket is full
     */
    void add(int ordinal, @Nonnull Object item);

    /**
     * Returns {@code true} if any of this outbox's buckets is full
     * (i.e., {@link #isFull(int)} would return true for it).
     */
    default boolean isFull() {
        return isFull(-1);
    }

    /**
     * Returns {@code true} if the bucket with the given ordinal is full.
     * When {@code true}, no more data can be added to the bucket during
     * the current invocation of the {@code Processor} method that made
     * the inquiry.
     * <p>
     * If {@code ordinal == -1}, behaves identically to {@link #isFull()}.
     */
    boolean isFull(int ordinal);
}
