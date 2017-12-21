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

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.JoinClause;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.SinkStage;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.StageWithGrouping;
import com.hazelcast.jet.StageWithTimestamp;
import com.hazelcast.jet.Transform;
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
import com.hazelcast.jet.function.DistributedToLongFunction;
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
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
public class ComputeStageImpl<T> extends AbstractStage implements ComputeStage<T> {

    ComputeStageImpl(@Nonnull Source<? extends T> source, @Nonnull PipelineImpl pipeline) {
        this(emptyList(), source, pipeline);
    }

    ComputeStageImpl(
            @Nullable ComputeStage upstream,
            @Nonnull Transform<? extends T> transform,
            @Nonnull PipelineImpl pipeline
    ) {
        this(singletonList(upstream), transform, pipeline);
    }

    ComputeStageImpl(
            @Nonnull List<ComputeStage> upstream,
            @Nonnull Transform<? extends T> transform,
            @Nonnull PipelineImpl pipeline
    ) {
        super(upstream, new ArrayList<>(), transform, pipeline);
    }

    @Nonnull @Override
    public <R> ComputeStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return attach(new MapTransform<>(mapFn));
    }

    @Nonnull @Override
    public ComputeStage<T> filter(@Nonnull DistributedPredicate<T> filterFn) {
        return attach(new FilterTransform<>(filterFn));
    }

    @Nonnull @Override
    public <R> ComputeStage<R> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attach(new FlatMapTransform<>(flatMapFn));
    }

    @Nonnull @Override
    public <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn) {
        return new StageWithGroupingImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public StageWithTimestamp<T> timestamp(@Nonnull DistributedToLongFunction<? super T> timestampFn) {
        return new StageWithTimestampImpl<>(this, timestampFn);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <K, T1_IN, T1> ComputeStage<Tuple2<T, T1>> hashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause
    ) {
        return attach(new HashJoinTransform<T>(singletonList(joinClause), emptyList()), singletonList(stage1));
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <K1, T1_IN, T1, K2, T2_IN, T2> ComputeStage<Tuple3<T, T1, T2>> hashJoin(
            @Nonnull ComputeStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull ComputeStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2
    ) {
        List<JoinClause<?, ? super T, ?, ?>> clauses = (List) asList(joinClause1, joinClause2);
        return attach(new HashJoinTransform<T>(clauses, emptyList()), asList(stage1, stage2));
    }

    @Nonnull @Override
    public <A, R> ComputeStage<R> aggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<T, A, R, R>(null, null, aggrOp));
    }

    @Nonnull @Override
    public <T1, A, R> ComputeStage<R> aggregate2(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return attach(new CoAggregateTransform<A, R, R>(aggrOp, null, null), singletonList(stage1));
    }

    @Nonnull @Override
    public <T1, T2, A, R> ComputeStage<R> aggregate3(
            @Nonnull ComputeStage<T1> stage1,
            @Nonnull ComputeStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return attach(new CoAggregateTransform<>(aggrOp, null, null), asList(stage1, stage2));
    }

    @Nonnull @Override
    public ComputeStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attach(new PeekTransform<>(shouldLogFn, toStringFn));
    }

    @Nonnull
    <R> ComputeStage<R> attach(@Nonnull UnaryTransform<? super T, ? extends R> unaryTransform) {
        return pipelineImpl.attach(unaryTransform, this);
    }

    @Nonnull
    <R> ComputeStage<R> attach(
            @Nonnull MultaryTransform<? extends R> multaryTransform,
            @Nonnull List<ComputeStage> otherInputs
    ) {
        return pipelineImpl.attach(multaryTransform,
                Stream.concat(Stream.of(this), otherInputs.stream()).collect(toList())
        );
    }

    @Nonnull @Override
    public SinkStage drainTo(@Nonnull Sink sink) {
        return pipelineImpl.drainTo(this, sink);
    }

    @Nonnull @Override
    public <R> ComputeStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier) {
        return attach(new ProcessorTransform<T, R>(stageName, procSupplier));
    }
}
