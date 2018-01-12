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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.GeneralComputeStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.StageWithGrouping;
import com.hazelcast.jet.pipeline.Transform;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.transform.MultaryTransform;
import com.hazelcast.jet.impl.pipeline.transform.UnaryTransform;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class ComputeStageImpl<T> extends ComputeStageImplBase<T> implements ComputeStage<T> {

    public ComputeStageImpl(
            @Nonnull List<? extends GeneralComputeStage> upstream,
            @Nonnull Transform<? extends T> transform,
            @Nonnull PipelineImpl pipeline
    ) {
        super(upstream, transform, true, pipeline);
    }

    ComputeStageImpl(@Nonnull Source<? extends T> source, @Nonnull PipelineImpl pipeline) {
        this(emptyList(), source, pipeline);
    }

    private ComputeStageImpl(
            @Nonnull GeneralComputeStage upstream,
            @Nonnull Transform<? extends T> transform,
            @Nonnull PipelineImpl pipeline
    ) {
        this(singletonList(upstream), transform, pipeline);
    }

    @Nonnull
    public <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn) {
        return new StageWithGroupingImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public <R> ComputeStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return attachMap(mapFn);
    }

    @Nonnull @Override
    public ComputeStage<T> filter(@Nonnull DistributedPredicate<T> filterFn) {
        return attachFilter(filterFn);
    }

    @Nonnull @Override
    public <R> ComputeStage<R> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMap(flatMapFn);
    }

    @Nonnull @Override
    public <K, T1_IN, T1> ComputeStage<Tuple2<T, T1>> hashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1
    ) {
        return attachHashJoin(stage1, joinClause1);
    }

    @Nonnull @Override
    public <K1, T1_IN, T1, K2, T2_IN, T2> ComputeStage<Tuple3<T, T1, T2>> hashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull ComputeStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2
    ) {
        return attachHashJoin(stage1, joinClause1, stage2, joinClause2);
    }

    @Nonnull @Override
    public <A, R> ComputeStage<R> aggregate(@Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp) {
        return attachAggregate(aggrOp);
    }

    @Nonnull @Override
    public <T1, A, R> ComputeStage<R> aggregate2(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return attachAggregate2(stage1, aggrOp);
    }

    @Nonnull @Override
    public <T1, T2, A, R> ComputeStage<R> aggregate3(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull ComputeStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return attachAggregate3(stage1, stage2, aggrOp);
    }

    @Nonnull @Override
    public ComputeStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attachPeek(shouldLogFn, toStringFn);
    }

    @Nonnull @Override
    public <R> ComputeStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attachCustomTransform(stageName, procSupplier);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, RET> RET attach(@Nonnull UnaryTransform<? super T, ? extends R> unaryTransform) {
        ComputeStageImpl<R> attached = new ComputeStageImpl<>(this, unaryTransform, pipelineImpl);
        pipelineImpl.connect(this, attached);
        return (RET) attached;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, RET> RET attach(
            @Nonnull MultaryTransform<R> multaryTransform,
            @Nonnull List<GeneralComputeStage> otherInputs
    ) {
        List<GeneralComputeStage> upstream =
                Stream.concat(Stream.of(this), otherInputs.stream()).collect(toList());
        ComputeStageImpl<R> attached = new ComputeStageImpl<>(upstream, multaryTransform, pipelineImpl);
        pipelineImpl.connect(upstream, attached);
        return (RET) attached;
    }
}
