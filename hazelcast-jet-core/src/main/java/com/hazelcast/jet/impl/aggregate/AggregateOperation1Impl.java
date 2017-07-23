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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.bag.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static com.hazelcast.jet.pipeline.bag.Tag.TAG_0;
import static com.hazelcast.jet.pipeline.bag.Tag.tag0;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Javadoc pending.
 */
public class AggregateOperation1Impl<T0, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation1<T0, A, R> {

    private final DistributedBiConsumer<? super A, ? super T0> accumulateItemF0;

    public AggregateOperation1Impl(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T0> accumulateItemF0,
                                   @Nonnull DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF, accumulatorsByTag(TAG_0, accumulateItemF0),
                combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        checkNotNull(accumulateItemF0, "accumulateItemF1");
        this.accumulateItemF0 = accumulateItemF0;
    }

    @Nonnull @Override
    public DistributedBiConsumer<? super A, ? super T0> accumulateItemF0() {
        return accumulateItemF0;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, T> accumulateItemF(Tag<T> tag) {
        if (tag != TAG_0) {
            throw new IllegalArgumentException("AggregateOperation1 recognizes only Tag.tag0()");
        }
        return (DistributedBiConsumer<? super A, T>) accumulateItemF0;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulateFsByTag(
            @Nonnull Map<Tag, DistributedBiConsumer<? super A, ?>> accumulateFsByTag
    ) {
        Tag<T0> tag = tag0();
        DistributedBiConsumer<? super A, T0> newAcc0 =
                (DistributedBiConsumer<? super A, T0>) accumulateFsByTag.get(tag);
        if (newAcc0 == null || accumulateFsByTag.size() != 1) {
            throw new IllegalArgumentException("AggregateOperation1#withAccumulatorsByTag()" +
                    " must get a map that has just one key: Tag.tag0().");
        }
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF0, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation1<T0, A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF0,
                combineAccumulatorsF(), deductAccumulatorF(), finishAccumulationF);
    }

    @Nonnull @Override
    public <T_NEW> AggregateOperation1<T_NEW, A, R> withAccumulateItemF1(
            DistributedBiConsumer<? super A, ? super T_NEW> accumulateItemF1
    ) {
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF1,
                combineAccumulatorsF(), deductAccumulatorF(), finishAccumulationF());
    }
}
