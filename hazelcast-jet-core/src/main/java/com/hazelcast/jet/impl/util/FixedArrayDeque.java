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

package com.hazelcast.jet.impl.util;

import java.util.ArrayDeque;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Array-deque with fixed capacity. Acts as a ring-buffer. Uses {@link
 * ArrayDeque} as the backing structure - allocated array length is {@code
 * nextPowerOfTwo(fixedCapacity + 1)} so choosing a capacity of
 *
 * @param <E>
 */
public class FixedArrayDeque<E> extends ArrayDeque<E> {

    private final int fixedCapacity;

    public FixedArrayDeque(int fixedCapacity) {
        super(fixedCapacity);
        checkPositive(fixedCapacity, "fixedCapacity=" + fixedCapacity + ", should be >0");
        this.fixedCapacity = fixedCapacity;
    }

    @Override
    public void addFirst(E e) {
        removeIfNeeded();
        super.addFirst(e);
    }

    @Override
    public void addLast(E e) {
        removeIfNeeded();
        super.addLast(e);
    }

    @Override
    public boolean offerFirst(E e) {
        removeIfNeeded();
        return super.offerFirst(e);
    }

    @Override
    public boolean offerLast(E e) {
        removeIfNeeded();
        return super.offerLast(e);
    }

    @Override
    public boolean add(E e) {
        removeIfNeeded();
        return super.add(e);
    }

    @Override
    public boolean offer(E e) {
        removeIfNeeded();
        return super.offer(e);
    }

    private void removeIfNeeded() {
        if (size() == fixedCapacity) {
            remove();
        }
    }
}
