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

package com.hazelcast.jet.pipeline.bag;

import java.io.Serializable;

/**
 * Javadoc pending.
 */
public class Tag<E> implements Serializable, Comparable<Tag<?>> {
    private static final Tag LEFT_TAG = new Tag(0);
    private static final Tag RIGHT_TAG = new Tag(1);

    private final int index;

    public Tag(int index) {
        this.index = index;
    }

    @SuppressWarnings("unchecked")
    public static <E> Tag<E> leftTag() {
        return LEFT_TAG;
    }

    @SuppressWarnings("unchecked")
    public static <E> Tag<E> righttTag() {
        return RIGHT_TAG;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj ||
                obj instanceof Tag && this.index == ((Tag) obj).index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public int compareTo(Tag<?> that) {
        return Integer.compare(this.index, that.index);
    }
}
