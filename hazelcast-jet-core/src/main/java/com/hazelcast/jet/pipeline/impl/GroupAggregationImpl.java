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

package com.hazelcast.jet.pipeline.impl;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.GroupAggregation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Javadoc pending.
 */
public class GroupAggregationImpl<B, A, R> implements GroupAggregation<B, A, R> {

    private final DistributedFunction<B, A> accumulateGroupF;
    private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
    private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;
    private final DistributedFunction<? super A, R> finishAccumulationF;

    public GroupAggregationImpl(
            DistributedFunction<B, A> accumulateGroupF,
            DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            DistributedFunction<? super A, R> finishAccumulationF
    ) {
        this.accumulateGroupF = accumulateGroupF;
        this.combineAccumulatorsF = combineAccumulatorsF;
        this.deductAccumulatorF = deductAccumulatorF;
        this.finishAccumulationF = finishAccumulationF;
    }

    @Override @Nonnull
    public DistributedFunction<B, A> accumulateGroupF() {
        return accumulateGroupF;
    }

    @Override @Nonnull
    public DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF() {
        return combineAccumulatorsF;
    }

    @Override @Nullable
    public DistributedBiConsumer<? super A, ? super A> deductAccumulatorF() {
        return deductAccumulatorF;
    }

    @Override @Nonnull
    public DistributedFunction<? super A, R> finishAccumulationF() {
        return finishAccumulationF;
    }
}
