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

import com.hazelcast.jet.impl.pipeline.transform.SourceImpl;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.StageWithGrouping;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
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

public class BatchStageImpl<T> extends ComputeStageImplBase<T> implements BatchStage<T> {

    public BatchStageImpl(
            @Nonnull List<? extends GeneralStage> upstream,
            @Nonnull Transform transform,
            @Nonnull PipelineImpl pipeline
    ) {
        super(upstream, transform, true, pipeline);
    }

    BatchStageImpl(@Nonnull SourceImpl<? extends T> source, @Nonnull PipelineImpl pipeline) {
        this(emptyList(), source, pipeline);
    }

    private BatchStageImpl(
            @Nonnull GeneralStage upstream,
            @Nonnull Transform transform,
            @Nonnull PipelineImpl pipeline
    ) {
        this(singletonList(upstream), transform, pipeline);
    }

    @Nonnull
    public <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn) {
        return new StageWithGroupingImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return attachMap(mapFn);
    }

    @Nonnull @Override
    public BatchStage<T> filter(@Nonnull DistributedPredicate<T> filterFn) {
        return attachFilter(filterFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMap(flatMapFn);
    }

    @Nonnull @Override
    public <K, T1_IN, T1> BatchStage<Tuple2<T, T1>> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1
    ) {
        return attachHashJoin(stage1, joinClause1);
    }

    @Nonnull @Override
    public <K1, T1_IN, T1, K2, T2_IN, T2> BatchStage<Tuple3<T, T1, T2>> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2
    ) {
        return attachHashJoin(stage1, joinClause1, stage2, joinClause2);
    }

    @Nonnull @Override
    public <A, R> BatchStage<R> aggregate(@Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp) {
        return attachAggregate(aggrOp);
    }

    @Nonnull @Override
    public <T1, A, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return attachAggregate2(stage1, aggrOp);
    }

    @Nonnull @Override
    public <T1, T2, A, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return attachAggregate3(stage1, stage2, aggrOp);
    }

    @Nonnull @Override
    public BatchStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attachPeek(shouldLogFn, toStringFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attachCustomTransform(stageName, procSupplier);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, RET> RET attach(@Nonnull UnaryTransform<? super T, ? extends R> unaryTransform) {
        BatchStageImpl<R> attached = new BatchStageImpl<>(this, unaryTransform, pipelineImpl);
        pipelineImpl.connect(this, attached);
        return (RET) attached;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, RET> RET attach(
            @Nonnull MultaryTransform<R> multaryTransform,
            @Nonnull List<GeneralStage> otherInputs
    ) {
        List<GeneralStage> upstream =
                Stream.concat(Stream.of(this), otherInputs.stream()).collect(toList());
        BatchStageImpl<R> attached = new BatchStageImpl<>(upstream, multaryTransform, pipelineImpl);
        pipelineImpl.connect(upstream, attached);
        return (RET) attached;
    }
}
