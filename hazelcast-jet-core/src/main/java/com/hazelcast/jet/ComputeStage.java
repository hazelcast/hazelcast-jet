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

package com.hazelcast.jet;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;

/**
 * Represents a pipeline in a distributed computation {@link Pipeline
 * pipeline}. It accepts input from its upstream stages (if any) and passes
 * its output to its downstream stages.
 *
 * @param <T> the type of items coming out of this pipeline
 */
public interface ComputeStage<T> extends GeneralComputeStage<T> {

    @Nonnull
    <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    @Nonnull @Override
    <R> ComputeStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    @Nonnull @Override
    ComputeStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    @Nonnull @Override
    <R> ComputeStage<R> flatMap(@Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn);

    @Nonnull @Override
    <K, T1_IN, T1> ComputeStage<Tuple2<T, T1>> hashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1
    );

    @Nonnull @Override
    <K1, T1_IN, T1, K2, T2_IN, T2> ComputeStage<Tuple3<T, T1, T2>> hashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull ComputeStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2
    );

    @Nonnull @Override
    default HashJoinBuilder<T> hashJoinBuilder() {
        return new HashJoinBuilder<>(this);
    }

    @Nonnull @Override
    ComputeStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    );

    @Nonnull @Override
    default ComputeStage<T> peek(@Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn) {
        return (ComputeStage<T>) GeneralComputeStage.super.peek(toStringFn);
    }

    @Nonnull @Override
    <R> ComputeStage<R> customTransform(
            @Nonnull String stageName, @Nonnull DistributedSupplier<Processor> procSupplier);

    @Nonnull
    <A, R> ComputeStage<R> aggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    );

    @Nonnull
    <T1, A, R> ComputeStage<R> aggregate2(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp);

    @Nonnull
    <T1, T2, A, R> ComputeStage<R> aggregate3(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull ComputeStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp);

    @Nonnull
    default AggregateBuilder<T> aggregateBuilder() {
        return new AggregateBuilder<>(this);
    }
}
