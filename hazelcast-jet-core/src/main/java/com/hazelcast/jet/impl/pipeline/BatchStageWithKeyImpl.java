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
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.impl.metrics.UserMetricsUtil;
import com.hazelcast.jet.impl.pipeline.transform.DistinctTransform;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DO_NOT_ADAPT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BatchStageWithKeyImpl<T, K> extends StageWithGroupingBase<T, K> implements BatchStageWithKey<T, K> {

    BatchStageWithKeyImpl(
            @Nonnull BatchStageImpl<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapStateful(0, createFn, mapFn, null);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        TriFunction<S, K, T, T> mapFn = (s, k, t) -> filterFn.test(s, t) ? t : null;
        return attachMapStateful(0, createFn, UserMetricsUtil.wrap(mapFn, filterFn), null);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapStateful(0, createFn, flatMapFn, null);
    }

    @Nonnull @Override
    public BatchStage<T> distinct() {
        return computeStage.attach(new DistinctTransform<>(computeStage.transform, keyFn()), DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingContext(contextFactory, mapFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        TriFunction<C, K, T, CompletableFuture<Traverser<R>>> flatMapAsyncFn =
                (c, k, t) -> mapAsyncFn.apply(c, k, t).thenApply(Traversers::singleton);
        return attachTransformUsingContextAsync("map", contextFactory,
                UserMetricsUtil.wrap(flatMapAsyncFn, mapAsyncFn));
    }

    @Nonnull @Override
    public <C> BatchStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriPredicate<? super C, ? super K, ? super T> filterFn
    ) {
        return attachFilterUsingContext(contextFactory, filterFn);
    }

    @Nonnull @Override
    public <C> BatchStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Boolean>>
                    filterAsyncFn
    ) {
        TriFunction<C, K, T, CompletableFuture<Traverser<T>>> flatMapAsyncFn = (c, k, t) ->
                filterAsyncFn.apply(c, k, t).thenApply(passed -> passed ? Traversers.singleton(t) : null);
        return attachTransformUsingContextAsync("filter", contextFactory,
                UserMetricsUtil.wrap(flatMapAsyncFn, filterAsyncFn));
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMapUsingContext(contextFactory, flatMapFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    ) {
        return attachTransformUsingContextAsync("flatMap", contextFactory, flatMapAsyncFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier) {
        return computeStage.attachPartitionedCustomTransform(stageName, procSupplier, keyFn());
    }

    @Nonnull @Override
    public <R> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return computeStage.attach(new GroupTransform<>(
                        singletonList(computeStage.transform),
                        singletonList(keyFn()),
                        aggrOp,
                        Util::entry),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public <T1, R> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, R> op
    ) {
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, transformOf(stage1)),
                        asList(keyFn(), stage1.keyFn()),
                        op,
                        Util::entry
                ), DO_NOT_ADAPT);
    }

    @Nonnull
    @Override
    public <T1, R0, R1> BatchStage<Entry<K, Tuple2<R0, R1>>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> op0,
            @Nonnull BatchStageWithKey<? extends T1, ? extends K> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> op1
    ) {
        AggregateOperation2<T, T1, ? extends Tuple2<?, ?>, Tuple2<R0, R1>> aggrOp = aggregateOperation2(op0, op1);
        return aggregate2(stage1, aggrOp);
    }

    @Nonnull @Override
    public <T1, T2, R> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull BatchStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> op
    ) {
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, transformOf(stage1), transformOf(stage2)),
                        asList(keyFn(), stage1.keyFn(), stage2.keyFn()),
                        op,
                        Util::entry),
                DO_NOT_ADAPT);
    }

    @Nonnull
    @Override
    public <T1, T2, R0, R1, R2> BatchStage<Entry<K, Tuple3<R0, R1, R2>>> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> op0,
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> op1,
            @Nonnull BatchStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> op2
    ) {
        AggregateOperation3<T, T1, T2, ? extends Tuple3<?, ?, ?>, Tuple3<R0, R1, R2>> aggrOp =
                aggregateOperation3(op0, op1, op2);
        return aggregate3(stage1, stage2, aggrOp);
    }
}
