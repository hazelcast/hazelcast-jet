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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;

/**
 * Specialization of {@link AggregateOperation} to the "arity-3" case with
 * two data stream being aggregated over.
 *
 * @param <T0> the type of item in stream-0
 * @param <T1> the type of item in stream-1
 * @param <T2> the type of item in stream-2
 * @param <A> the type of the accumulator
 * @param <R> the type of the aggregation result
 */
public interface AggregateOperation3<T0, T1, T2, A, R> extends AggregateOperation<A, R> {

    /**
     * A primitive that updates the accumulator state to account for a new
     * item coming from stream-0.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super T0> accumulateFn0();

    /**
     * A primitive that updates the accumulator state to account for a new
     * item coming from stream-1.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super T1> accumulateFn1();

    /**
     * A primitive that updates the accumulator state to account for a new
     * item coming from stream-2.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super T2> accumulateFn2();

    /**
     * Javadoc pending
     */
    @Nonnull
    <T0_NEW> AggregateOperation3<T0_NEW, T1, T2, A, R> withAccumulateFn0(
            @Nonnull DistributedBiConsumer<? super A, ? super T0_NEW> newAccFn0
    );

    /**
     * Javadoc pending
     */
    @Nonnull
    <T1_NEW> AggregateOperation3<T0, T1_NEW, T2, A, R> withAccumulateFn1(
            @Nonnull DistributedBiConsumer<? super A, ? super T1_NEW> newAccFn1
    );

    /**
     * Javadoc pending
     */
    @Nonnull
    <T2_NEW> AggregateOperation3<T0, T1, T2_NEW, A, R> withAccumulateFn2(
            @Nonnull DistributedBiConsumer<? super A, ? super T2_NEW> newAccFn2
    );

    // Override with a narrowed return type
    @Nonnull @Override
    <R1> AggregateOperation3<T0, T1, T2, A, R1> withFinishFn(
            @Nonnull DistributedFunction<? super A, R1> finishFn
    );
}
