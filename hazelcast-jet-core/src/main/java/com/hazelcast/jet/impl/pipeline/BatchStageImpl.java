/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.metrics.UserMetricsUtil;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BatchStageImpl<T> extends ComputeStageImplBase<T> implements BatchStage<T> {

    BatchStageImpl(@Nonnull Transform transform, @Nonnull PipelineImpl pipeline) {
        super(transform, DO_NOT_ADAPT, pipeline, true);
    }

    /**
     * This constructor exists just to match the shape of the functional interface
     * {@code GeneralHashJoinBuilder.CreateOutStageFn}
     */
    public BatchStageImpl(@Nonnull Transform transform, FunctionAdapter ignored, @Nonnull PipelineImpl pipeline) {
        this(transform, pipeline);
    }

    @Nonnull @Override
    public <K> BatchStageWithKey<T, K> groupingKey(@Nonnull FunctionEx<? super T, ? extends K> keyFn) {
        checkSerializable(keyFn, "keyFn");
        return new BatchStageWithKeyImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> map(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        return attachMap(mapFn);
    }

    @Nonnull @Override
    public BatchStage<T> filter(@Nonnull PredicateEx<T> filterFn) {
        return attachFilter(filterFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> flatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMap(flatMapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return attachGlobalMapStateful(createFn, mapFn);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        BiFunctionEx<? super S, ? super T, ? extends T> mapFn = (s, t) -> filterFn.test(s, t) ? t : null;
        return attachGlobalMapStateful(createFn, UserMetricsUtil.wrap(mapFn, filterFn));
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachGlobalFlatMapStateful(createFn, flatMapFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingContext(contextFactory, mapFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        BiFunctionEx<C, T, CompletableFuture<Traverser<R>>> flatMapAsyncFn =
                (c, t) -> mapAsyncFn.apply(c, t).thenApply(Traversers::singleton);
        return attachFlatMapUsingContextAsync("map", contextFactory,
                UserMetricsUtil.wrap(flatMapAsyncFn, mapAsyncFn));
    }

    @Nonnull @Override
    public <C> BatchStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiPredicateEx<? super C, ? super T> filterFn
    ) {
        return attachFilterUsingContext(contextFactory, filterFn);
    }

    @Nonnull @Override
    public <C> BatchStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends CompletableFuture<Boolean>> filterAsyncFn
    ) {
        BiFunctionEx<C, T, CompletableFuture<Traverser<T>>> flatMapAsyncFn = (c, t) -> filterAsyncFn.apply(c, t)
                .thenApply(passed -> passed ? Traversers.singleton(t) : null);
        return attachFlatMapUsingContextAsync("filter", contextFactory,
                UserMetricsUtil.wrap(flatMapAsyncFn, filterAsyncFn));
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapUsingContext(contextFactory, flatMapFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        return attachFlatMapUsingContextAsync("flatMap", contextFactory, flatMapAsyncFn);
    }

    @Nonnull @Override
    public BatchStage<T> merge(@Nonnull BatchStage<? extends T> other) {
        return attachMerge(other);
    }

    @Nonnull @Override
    public <K, T1_IN, T1, R> BatchStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    ) {
        return attachHashJoin(stage1, joinClause1, mapToOutputFn);
    }

    @Nonnull @Override
    public <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return attachHashJoin2(stage1, joinClause1, stage2, joinClause2, mapToOutputFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> aggregate(@Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp) {
        List<Serializable> metricsProviderCandidates = asList(
                aggrOp, aggrOp.accumulateFn(), aggrOp.createFn(), aggrOp.combineFn(), aggrOp.deductFn(), aggrOp.exportFn()
        );
        AggregateOperation1<? super T, ?, ? extends R> wrappedAggrOp =
                UserMetricsUtil.wrapAll(aggrOp, metricsProviderCandidates);
        return attach(new AggregateTransform<>(singletonList(transform), wrappedAggrOp), fnAdapter);
    }

    @Nonnull @Override
    public <T1, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    ) {
        List<Serializable> metricsProviderCandidates = asList(
                aggrOp, aggrOp.accumulateFn0(), aggrOp.accumulateFn1(),
                aggrOp.createFn(), aggrOp.combineFn(), aggrOp.deductFn(), aggrOp.exportFn()
        );
        AggregateOperation2<? super T, ? super T1, ?, ? extends R> wrappedAggrOp =
                UserMetricsUtil.wrapAll(aggrOp, metricsProviderCandidates);
        return attach(new AggregateTransform<>(asList(transform, transformOf(stage1)), wrappedAggrOp), DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public <T1, R0, R1> BatchStage<Tuple2<R0, R1>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> op0,
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> op1) {
        List<Serializable> metricsProviderCandidates = asList(
                op0, op0.accumulateFn(), op0.createFn(), op0.combineFn(), op0.deductFn(), op0.exportFn(),
                op1, op1.accumulateFn(), op1.createFn(), op1.combineFn(), op1.deductFn(), op1.exportFn()
        );
        AggregateOperation2<T, T1, ? extends Tuple2<?, ?>, Tuple2<R0, R1>> aggrOp = aggregateOperation2(op0, op1);
        return aggregate2(stage1, UserMetricsUtil.wrapAll(aggrOp, metricsProviderCandidates));
    }

    @Nonnull @Override
    public <T1, T2, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<>(
                asList(transform, transformOf(stage1), transformOf(stage2)), aggrOp),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public BatchStage<T> peek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        return attachPeek(shouldLogFn, toStringFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorMetaSupplier procSupplier
    ) {
        return attachCustomTransform(stageName, procSupplier);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <RET> RET attach(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter) {
        pipelineImpl.connect(transform.upstream(), transform);
        return (RET) new BatchStageImpl<>(transform, pipelineImpl);
    }

    @Nonnull @Override
    public BatchStage<T> setLocalParallelism(int localParallelism) {
        super.setLocalParallelism(localParallelism);
        return this;
    }

    @Nonnull @Override
    public BatchStage<T> setName(@Nonnull String name) {
        super.setName(name);
        return this;
    }
}
