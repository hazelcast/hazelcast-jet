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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;
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
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
public abstract class ComputeStageImplBase<T> extends AbstractStage {

    public static final FunctionAdapters DONT_ADAPT = new FunctionAdapters();
    static final FunctionAdapters ADAPT_TO_JET_EVENT = new JetEventFunctionAdapters();

    @Nonnull
    public final FunctionAdapters fnAdapters;

    ComputeStageImplBase(
            @Nonnull Transform transform,
            @Nonnull FunctionAdapters fnAdapters,
            @Nonnull PipelineImpl pipelineImpl,
            boolean acceptsDownstream
    ) {
        super(transform, acceptsDownstream, pipelineImpl);
        this.fnAdapters = fnAdapters;
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
                ADAPT_TO_JET_EVENT,
                pipelineImpl);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachMap(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return (RET) attach(new MapTransform(this.transform, fnAdapters.adaptMapFn(mapFn)), fnAdapters);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachFilter(@Nonnull DistributedPredicate<T> filterFn) {
        return (RET) attach(new FilterTransform(transform, fnAdapters.adaptFilterFn(filterFn)), fnAdapters);
    }

    @Nonnull
    <R, RET> RET attachFlatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attach(new FlatMapTransform<>(transform, fnAdapters.adaptFlatMapFn(flatMapFn)), fnAdapters);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K1, T1_IN, T1, R, RET> RET attachHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (RET) attach(new HashJoinTransform<>(
                asList(transform, transformOf(stage1)),
                singletonList(fnAdapters.adaptJoinClause(joinClause)),
                emptyList(),
                fnAdapters.adaptMapToOutputFn(mapToOutputFn)
        ), fnAdapters);
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
        return (RET) attach(new HashJoinTransform<>(
                asList(transform, transformOf(stage1), transformOf(stage2)),
                asList(fnAdapters.adaptJoinClause(joinClause1), fnAdapters.adaptJoinClause(joinClause2)),
                emptyList(),
                fnAdapters.adaptMapToOutputFn(mapToOutputFn)
        ), fnAdapters);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <A, R, RET> RET attachAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        AggregateOperation1 adapted = ((AggregateOperation1) aggrOp)
                .withAccumulateFn(fnAdapters.adaptAccumulateFn(aggrOp.accumulateFn()));
        return attach(new AggregateTransform<T, A, R>(transform, adapted), fnAdapters);
    }

    @Nonnull
    <T1, A, R, RET> RET attachAggregate2(
            @Nonnull GeneralStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        ComputeStageImplBase rawStage1 = (ComputeStageImplBase) stage1;
        AggregateOperationImpl rawAggrOp = (AggregateOperationImpl) aggrOp;
        DistributedBiConsumer[] adaptedAccFns = {
                fnAdapters.adaptAccumulateFn(rawAggrOp.accumulateFn(0)),
                rawStage1.fnAdapters.adaptAccumulateFn(rawAggrOp.accumulateFn(1))
        };
        AggregateOperation<?, ?> adaptedAggrOp = rawAggrOp.withAccumulateFns(adaptedAccFns);
        return attach(new CoAggregateTransform<>(asList(transform, transformOf(stage1)), adaptedAggrOp), DONT_ADAPT);
    }

    @Nonnull
    <T1, T2, A, R, RET> RET attachAggregate3(
            @Nonnull GeneralStage<T1> stage1,
            @Nonnull GeneralStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        ComputeStageImplBase rawStage1 = (ComputeStageImplBase) stage1;
        ComputeStageImplBase rawStage2 = (ComputeStageImplBase) stage2;
        AggregateOperationImpl rawAggrOp = (AggregateOperationImpl) aggrOp;
        DistributedBiConsumer[] adaptedAccFns = {
                fnAdapters.adaptAccumulateFn(rawAggrOp.accumulateFn(0)),
                rawStage1.fnAdapters.adaptAccumulateFn(rawAggrOp.accumulateFn(1)),
                rawStage2.fnAdapters.adaptAccumulateFn(rawAggrOp.accumulateFn(2))
        };
        AggregateOperation<?, ?> adaptedAggrOp = rawAggrOp.withAccumulateFns(adaptedAccFns);
        return attach(new CoAggregateTransform<>(
                asList(transform, transformOf(stage1), transformOf(stage2)), adaptedAggrOp), DONT_ADAPT);
    }

    @Nonnull
    <RET> RET attachPeek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attach(new PeekTransform<>(transform, shouldLogFn, toStringFn), DONT_ADAPT);
    }

    @Nonnull
    <RET, R> RET attachCustomTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attach(new ProcessorTransform(transform, stageName, procSupplier), fnAdapters);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public SinkStage drainTo(@Nonnull Sink<? super T> sink) {
        return pipelineImpl.drain(this.transform, sink);
    }

    @Nonnull
    abstract <RET> RET attach(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapters fnAdapters);
}
