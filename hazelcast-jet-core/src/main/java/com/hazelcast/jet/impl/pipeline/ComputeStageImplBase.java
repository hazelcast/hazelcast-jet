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

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.JoinClause;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.SinkStage;
import com.hazelcast.jet.Transform;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.FilterTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.MultaryTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform;
import com.hazelcast.jet.impl.pipeline.transform.UnaryTransform;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
public abstract class ComputeStageImplBase<T> extends AbstractStage {
    ComputeStageImplBase(
            List<? extends ComputeStageImplBase> upstream,
            Transform transform, boolean acceptsDownstream,
            PipelineImpl pipelineImpl
    ) {
        super(upstream, transform, acceptsDownstream, pipelineImpl);
    }

    @Nonnull
    <R, RET> RET attachMap(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return attach(new MapTransform<>(mapFn));
    }

    @Nonnull
    <RET> RET attachFilter(@Nonnull DistributedPredicate<T> filterFn) {
        return attach(new FilterTransform<>(filterFn));
    }

    @Nonnull
    <R, RET> RET attachFlatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attach(new FlatMapTransform<>(flatMapFn));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K, T1_IN, T1, RET> RET attachHashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause
    ) {
        return attach(
                new HashJoinTransform<>(singletonList(joinClause), emptyList()),
                singletonList((ComputeStageImplBase) stage1));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K1, T1_IN, T1, K2, T2_IN, T2, RET> RET attachHashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull ComputeStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2
    ) {
        List<JoinClause<?, ? super T, ?, ?>> clauses = (List) asList(joinClause1, joinClause2);
        return attach(new HashJoinTransform<>(clauses, emptyList()),
                asList((ComputeStageImplBase) stage1, (ComputeStageImplBase) stage2));
    }

    @Nonnull
    <A, R, RET> RET attachAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<T, A, R, R>(null, aggrOp));
    }

    @Nonnull
    <T1, A, R, RET> RET attachAggregate2(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return attach(new CoAggregateTransform<A, R, R>(aggrOp, null), singletonList((ComputeStageImplBase) stage1));
    }

    @Nonnull
    <T1, T2, A, R, RET> RET attachAggregate3(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull ComputeStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return attach(new CoAggregateTransform<>(aggrOp, null),
                asList((ComputeStageImplBase) stage1, (ComputeStageImplBase) stage2));
    }

    @Nonnull
    <RET> RET attachPeek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attach(new PeekTransform<>(shouldLogFn, toStringFn));
    }

    @Nonnull
    <RET, R> RET attachCustomTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attach(new ProcessorTransform<T, R>(stageName, procSupplier));
    }

    @Nonnull
    public SinkStage drainTo(@Nonnull Sink<? super T> sink) {
        return pipelineImpl.drainTo(this, sink);
    }

    @Nonnull
    abstract <R, RET> RET attach(@Nonnull UnaryTransform<? super T, ? extends R> unaryTransform);

    @Nonnull
    abstract <R, RET> RET attach(
            @Nonnull MultaryTransform<R> multaryTransform,
            @Nonnull List<ComputeStageImplBase> otherInputs
    );
}
