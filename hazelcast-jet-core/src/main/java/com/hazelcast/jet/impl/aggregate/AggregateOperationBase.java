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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.bag.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

abstract class AggregateOperationBase<A, R> implements Serializable {
    private final DistributedSupplier<A> createAccumulatorF;
    private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
    private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;
    private final DistributedFunction<? super A, R> finishAccumulationF;

    AggregateOperationBase(
            @Nonnull DistributedSupplier<A> createAccumulatorF,
            @Nonnull DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        checkNotNull(createAccumulatorF, "createAccumulatorF");
        checkNotNull(createAccumulatorF, "combineAccumulatorsF");
        checkNotNull(createAccumulatorF, "finishAccumulationF");
        this.createAccumulatorF = createAccumulatorF;
        this.combineAccumulatorsF = combineAccumulatorsF;
        this.deductAccumulatorF = deductAccumulatorF;
        this.finishAccumulationF = finishAccumulationF;
    }

    @Nonnull
    public DistributedSupplier<A> createAccumulatorF() {
        return createAccumulatorF;
    }

    @Nonnull
    public DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF() {
        return combineAccumulatorsF;
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> deductAccumulatorF() {
        return deductAccumulatorF;
    }

    @Nonnull
    public DistributedFunction<? super A, R> finishAccumulationF() {
        return finishAccumulationF;
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
