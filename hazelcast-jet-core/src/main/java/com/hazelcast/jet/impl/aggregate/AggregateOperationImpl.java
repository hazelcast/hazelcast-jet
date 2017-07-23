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

package com.hazelcast.jet.impl.aggregate;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.bag.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class AggregateOperationImpl<A, R> extends AggregateOperationBase<A, R> implements AggregateOperation<A, R> {
    private final Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag;

    public AggregateOperationImpl(
            @Nonnull DistributedSupplier<A> createAccumulatorF,
            Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag,
            @Nonnull DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        this.accumulatorsByTag = accumulatorsByTag;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, T> accumulateItemF(Tag<T> tag) {
        DistributedBiConsumer<? super A, T> acc = (DistributedBiConsumer<? super A, T>) accumulatorsByTag.get(tag);
        if (acc == null) {
            throw new IllegalArgumentException("The provided tag is not registered with this AggregateOperation.");
        }
        return acc;
    }

    @Nonnull @Override
    public AggregateOperation<A, R> withAccumulatorsByTag(
            @Nonnull Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag
    ) {
        return new AggregateOperationImpl<>(
                createAccumulatorF(), accumulatorsByTag, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation<A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperationImpl<>(
                createAccumulatorF(), accumulatorsByTag, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <A> Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag(Object... tagsAndAccs) {
        Map<Tag, DistributedBiConsumer<? super A, ?>> map = new HashMap<>();
        for (int i = 0; i < tagsAndAccs.length;) {
            map.put((Tag) tagsAndAccs[i++],
                    (DistributedBiConsumer<? super A, ?>) tagsAndAccs[i++]);
        }
        return map;
    }

}
