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

import static com.hazelcast.jet.pipeline.bag.Tag.tag1;

/**
 * Javadoc pending.
 */
public class AggregateOperation1Impl<T1, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation1<T1, A, R> {

    private final DistributedBiConsumer<? super A, ? super T1> accumulateItemF1;

    public AggregateOperation1Impl(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T1> accumulateItemF1,
                                   @Nonnull DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF, accumulatorsByTag(tag1(), accumulateItemF1),
                combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        this.accumulateItemF1 = accumulateItemF1;
    }

    @Nonnull @Override
    public DistributedBiConsumer<? super A, ? super T1> accumulateItemF1() {
        return accumulateItemF1;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulatorsByTag(
            @Nonnull Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag
    ) {
        Tag<T1> tag = tag1();
        DistributedBiConsumer<? super A, T1> newAcc1 =
                (DistributedBiConsumer<? super A, T1>) accumulatorsByTag.get(tag);
        if (newAcc1 == null || accumulatorsByTag.size() != 1) {
            throw new IllegalArgumentException("AggregateOperation1#withAccumulatorsByTag()" +
                    " must get a map that has just one key: Tag.tag1().");
        }
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF1, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation1<T1, A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF1,
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
