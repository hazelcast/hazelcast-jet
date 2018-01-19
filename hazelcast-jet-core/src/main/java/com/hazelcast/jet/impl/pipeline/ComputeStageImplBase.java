/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.FilterTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
public abstract class ComputeStageImplBase<T> extends AbstractStage {

    ComputeStageImplBase(
            @Nonnull Transform transform,
            @Nonnull PipelineImpl pipelineImpl,
            boolean acceptsDownstream
    ) {
        super(transform, acceptsDownstream, pipelineImpl);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public StreamStage<T> timestamp(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull WatermarkPolicy wmPolicy
    ) {
        return new StreamStageImpl<>(
                new TimestampTransform<>(transform,
                        wmGenParams(timestampFn, wmPolicy, suppressDuplicates(), 0L)),
                pipelineImpl);
    }

    @Nonnull
    <R, RET> RET attachMap(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return attach(new MapTransform<>(transform, mapFn));
    }

    @Nonnull
    <RET> RET attachFilter(@Nonnull DistributedPredicate<T> filterFn) {
        return attach(new FilterTransform<>(transform, filterFn));
    }

    @Nonnull
    <R, RET> RET attachFlatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attach(new FlatMapTransform<>(transform, flatMapFn));
    }

    @Nonnull
    <K, T1_IN, T1, R, RET> RET attachHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return attach(new HashJoinTransform<>(
                asList(transform, transformOf(stage1)),
                singletonList(joinClause),
                emptyList(),
                (T t0, ItemsByTag t) -> mapToOutputFn.apply(t0, t.get(tag1()))
        ));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K1, T1_IN, T1, K2, T2_IN, T2, R, RET> RET attachHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        List<JoinClause<?, ? super T, ?, ?>> clauses = (List) asList(joinClause1, joinClause2);
        return attach(new HashJoinTransform<>(
                asList(transform, transformOf(stage1), transformOf(stage2)),
                clauses,
                emptyList(),
                (T t0, ItemsByTag t) -> mapToOutputFn.apply(t0, t.get(tag1()), t.get(tag2()))
        ));
    }

    @Nonnull
    <A, R, RET> RET attachAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<T, A, R>(transform, aggrOp));
    }

    @Nonnull
    <T1, A, R, RET> RET attachAggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return attach(new CoAggregateTransform<A, R, R>(asList(transform, transformOf(stage1)), aggrOp));
    }

    @Nonnull
    <T1, T2, A, R, RET> RET attachAggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return attach(new CoAggregateTransform<>(
                asList(transform, transformOf(stage1), transformOf(stage2)), aggrOp));
    }

    @Nonnull
    <RET> RET attachPeek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attach(new PeekTransform<>(transform, shouldLogFn, toStringFn));
    }

    @Nonnull
    <RET, R> RET attachCustomTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attach(new ProcessorTransform(transform, stageName, procSupplier));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public SinkStage drainTo(@Nonnull Sink<? super T> sink) {
        return pipelineImpl.drain(this.transform, sink);
    }

    @Nonnull
    abstract <RET> RET attach(@Nonnull AbstractTransform transform);
}
