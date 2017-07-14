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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.bag.BagsByTag;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.bag.TwoBags;
import com.hazelcast.jet.pipeline.impl.GroupAggregationImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @param <B> the type that holds bags of grouped values. May be {@link TwoBags},
 *            {@link ThreeBags} or {@link BagsByTag}.
 * @param <A> the type that holds the accumulated value
 * @param <R> the type of the final result
 */
public interface GroupAggregation<B, A, R> {

    @Nonnull
    DistributedFunction<B, A> accumulateGroupF();

    @Nonnull
    DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF();

    @Nullable
    DistributedBiConsumer<? super A, ? super A> deductAccumulatorF();

    @Nonnull
    DistributedFunction<? super A, R> finishAccumulationF();

    static <E1, E2, A, R> GroupAggregation<TwoBags<E1, E2>, A, R> of(
            DistributedFunction<TwoBags<E1, E2>, A> accumulateGroupF,
            DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            DistributedFunction<? super A, R> finishAccumulationF
    ) {
        return new GroupAggregationImpl<>(
                accumulateGroupF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }

    static <E1, E2, E3, A, R> GroupAggregation<ThreeBags<E1, E2, E3>, A, R> of3(
            DistributedFunction<ThreeBags<E1, E2, E3>, A> accumulateGroupF,
            DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            DistributedFunction<? super A, R> finishAccumulationF
    ) {
        return new GroupAggregationImpl<>(
                accumulateGroupF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }

    static <A, R> GroupAggregation<BagsByTag, A, R> ofMany(
            DistributedFunction<BagsByTag, A> accumulateGroupF,
            DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            DistributedFunction<? super A, R> finishAccumulationF
    ) {
        return new GroupAggregationImpl<>(
                accumulateGroupF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }
}
