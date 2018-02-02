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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.util.function.IntUnaryOperator;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link IntUnaryOperator
 * java.util.function.IntUnaryOperator}.
 */
@FunctionalInterface
public interface DistributedIntUnaryOperator extends IntUnaryOperator, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * IntUnaryOperator#identity() java.util.function.IntUnaryOperator#identity()}.
     */
    static DistributedIntUnaryOperator identity() {
        return n -> n;
    }

    /**
     * {@code Serializable} variant of {@link
     * IntUnaryOperator#compose(IntUnaryOperator)
     * java.util.function.IntUnaryOperator#compose(IntUnaryOperator)}.
     */
    default DistributedIntUnaryOperator compose(DistributedIntUnaryOperator before) {
        checkNotNull(before, "before");
        return n -> applyAsInt(before.applyAsInt(n));
    }

    /**
     * {@code Serializable} variant of {@link
     * IntUnaryOperator#andThen(IntUnaryOperator)
     * java.util.function.IntUnaryOperator#andThen(IntUnaryOperator)}.
     */
    default DistributedIntUnaryOperator andThen(DistributedIntUnaryOperator after) {
        checkNotNull(after, "after");
        return n -> after.applyAsInt(applyAsInt(n));
    }
}
