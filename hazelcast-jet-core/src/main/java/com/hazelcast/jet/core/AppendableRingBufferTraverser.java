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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.util.QuickMath;

import javax.annotation.Nonnull;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * A traverser with an internal ring-buffer. You can efficiently {@link
 * #append} items to it that will be later traversed using {@link #next}.
 * <p>
 * It's useful to be returned from a flat-mapping function when an item is
 * flat-mapped to small number of items:
 * <pre>{@code
 *     AppendableRingBufferTraverser rt = new AppendableRingBufferTraverser(2);
 *     Traverser t = Traverser.over(10, 20)
 *         .flatMap(item -> {
 *             rt.push(item);
 *             rt.push(item + 1);
 *             return rt;
 *         });
 * }</pre>
 * The {@code t} traverser above will output {10, 11, 20, 21}. This approach
 * reduces the GC pressure by avoiding the allocation of new traverser that
 * will traverse over 1 or few or even zero items.
 *
 * <p>
 * See {@link ResettableSingletonTraverser} if you have at most one item to
 * traverse.
 *
 * @param <T> item type
 */
public class AppendableRingBufferTraverser<T> implements Traverser<T> {
    // package-visible for test
    int head;
    int tail;

    private final Object[] array;
    private final int mask;
    private final int capacity;

    /**
     * Creates a ring-buffer backed traverser.
     *
     * @param capacity maximum capacity, will be rounded up to next power of 2
     */
    public AppendableRingBufferTraverser(int capacity) {
        checkPositive(capacity, "capacity <= 0");
        capacity = QuickMath.nextPowerOfTwo(capacity);
        this.capacity = capacity;
        array = new Object[capacity];
        mask = capacity - 1;
    }

    @Nonnull
    @Override
    public Traverser<T> append(@Nonnull T item) {
        if (tail - head == capacity) {
            throw new IllegalStateException("ringbuffer full");
        }
        array[tail++ & mask] = item;
        return this;
    }

    @Override
    public T next() {
        if (isEmpty()) {
            return null;
        }
        return (T) array[head++ & mask];
    }

    /**
     * Returns {@code true}, if the next call to {@link #next} will return
     * {@code null}.
     */
    public boolean isEmpty() {
        return head == tail;
    }
}
