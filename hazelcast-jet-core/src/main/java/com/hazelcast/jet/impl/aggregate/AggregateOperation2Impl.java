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
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.bag.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static com.hazelcast.jet.pipeline.bag.Tag.TAG_1;
import static com.hazelcast.jet.pipeline.bag.Tag.TAG_2;
import static com.hazelcast.jet.pipeline.bag.Tag.tag1;
import static com.hazelcast.jet.pipeline.bag.Tag.tag2;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Javadoc pending.
 */
public class AggregateOperation2Impl<T1, T2, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation2<T1, T2, A, R> {

    private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
    private final DistributedBiConsumer<? super A, T2> accumulateItemF2;

    public AggregateOperation2Impl(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                   @Nonnull DistributedBiConsumer<? super A, T1> accumulateItemF1,
                                   @Nonnull DistributedBiConsumer<? super A, T2> accumulateItemF2,
                                   @Nonnull DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF,
                accumulatorsByTag(TAG_1, accumulateItemF1, TAG_2, accumulateItemF2),
                combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        checkNotNull(accumulateItemF1, "accumulateItemF1");
        checkNotNull(accumulateItemF2, "accumulateItemF2");
        this.accumulateItemF1 = accumulateItemF1;
        this.accumulateItemF2 = accumulateItemF2;
    }

    @Nonnull @Override
    public DistributedBiConsumer<? super A, ? super T1> accumulateItemF1() {
        return accumulateItemF1;
    }

    @Nonnull @Override
    public DistributedBiConsumer<? super A, ? super T2> accumulateItemF2() {
        return accumulateItemF2;
    }

    @Nonnull @Override
    public <T> DistributedBiConsumer<? super A, T> accumulateItemF(Tag<T> tag) {
        DistributedBiConsumer<? super A, ?> accF =
                tag == TAG_1 ? accumulateItemF1
              : tag == TAG_2 ? accumulateItemF2
              : null;
        if (accF == null) {
            throw new IllegalArgumentException(
                    "AggregateOperation2 recognizes only Tag.tag1() and Tag.tag2()");
        }
        return (DistributedBiConsumer<? super A, T>) accF;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulatorsByTag(
            @Nonnull Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag
    ) {
        Tag<T1> tag1 = tag1();
        Tag<T2> tag2 = tag2();
        DistributedBiConsumer<? super A, T1> newAcc1 =
                (DistributedBiConsumer<? super A, T1>) accumulatorsByTag.get(tag1);
        DistributedBiConsumer<? super A, T2> newAcc2 =
                (DistributedBiConsumer<? super A, T2>) accumulatorsByTag.get(tag2);
        if (newAcc1 == null || newAcc2 == null || accumulatorsByTag.size() != 2) {
            throw new IllegalArgumentException("AggregateOperation2#withAccumulatorsByTag()" +
                    " must get a map that has exactly two keys: Tag.tag1() and Tag.tag2().");
        }
        return new AggregateOperation2Impl<>(
                createAccumulatorF(), newAcc1, newAcc2, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation2<T1, T2, A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperation2Impl<>(
                createAccumulatorF(), accumulateItemF1, accumulateItemF2,
                combineAccumulatorsF(), deductAccumulatorF(), finishAccumulationF);
    }
}
