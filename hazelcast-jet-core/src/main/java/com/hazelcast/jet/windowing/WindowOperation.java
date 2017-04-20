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
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.stream.DistributedCollector;

import java.io.Serializable;
import java.util.Objects;

/**
 * Contains functions needed to compute a windowed result of infinite
 * stream processing by storing intermediate results in a mutable result
 * container, called the <em>accumulator</em>. These are the supported
 * operations:
 * <ol><li>
 *     {@link #createAccumulatorF() create} a new accumulator
 * </li><li>
 *     {@link #accumulateItemF() accumulate} the data of an item
 * </li><li>
 *     {@link #combineAccumulatorsF() combine} the contents of two accumulator
 *     objects
 * </li><li>
 *     {@link #deductAccumulatorF() deduct} the contents of an accumulator from
 *     another
 * </li><li>
 *     {@link #finishAccumulationF() finish} accumulation by transforming the
 *     accumulator's intermediate result into the final result
 * </li></ol>
 *
 * @param <T> the type of the stream item
 * @param <A> the type of the accumulator
 * @param <R> the type of the final result
 */
public interface WindowOperation<T, A, R> extends Serializable {

    /**
     * A function that creates a new accumulator and returns it. If the {@code
     * deduct} operation is defined, the accumulator object must properly
     * implement {@code equals()}, which will be used to detect when an
     * accumulator is "empty" and can be evicted from a processor's storage.
     */
    Distributed.Supplier<A> createAccumulatorF();

    /**
     * A function that updates the accumulated value to account for a new item.
     */
    Distributed.BiConsumer<A, T> accumulateItemF();

    /**
     * A function that accepts two accumulators, merges their contents, and
     * returns an accumulator with the resulting state. It is allowed to mutate
     * the left-hand operator (presumably to return it as the new result), but
     * not the right-hand one.
     */
    Distributed.BinaryOperator<A> combineAccumulatorsF();

    /**
     * A function that accepts two accumulators, deducts the contents of the
     * right-hand one from the contents of the left-hand one, and returns an
     * accumulator with the resulting state. It is allowed to mutate the
     * left-hand accumulator (presumably to return it as the new result), but
     * not the right-hand one.
     */
    BinaryOperator<A> deductAccumulatorF();

    /**
     * A function that finishes the accumulation process by transforming
     * the accumulator object into the final result.
     */
    Distributed.Function<A, R> finishAccumulationF();

    /**
     * Returns a new {@code WindowingFunctions} object composed from the
     * provided functions.
     *
     * @param <T> the type of the stream item
     * @param <A> the type of the accumulator
     * @param <R> the type of the final result
     */
    static <T, A, R> WindowOperation<T, A, R> of(Distributed.Supplier<A> createAccumulatorF,
                                                 Distributed.BiConsumer<A, T> accumulateItemF,
                                                 Distributed.BinaryOperator<A> combineAccumulatorsF,
                                                 Distributed.BinaryOperator<A> deductAccumulatorF,
                                                 Distributed.Function<A, R> finishAccumulationF
    ) {
        Objects.requireNonNull(createAccumulatorF);
        Objects.requireNonNull(accumulateItemF);
        Objects.requireNonNull(combineAccumulatorsF);
        Objects.requireNonNull(finishAccumulationF);
        return new WindowOperationImpl<>(
                createAccumulatorF, accumulateItemF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }

    /**
     * Returns a new {@code WindowingFunctions} object based on a
     * {@code DistributedCollector}.
     */
    static <T, A, R> WindowOperation<T, A, R> fromCollector(DistributedCollector<T, A, R> c) {
        return of(c.supplier(), c.accumulator(), c.combiner(), null, c.finisher());
    }
}
