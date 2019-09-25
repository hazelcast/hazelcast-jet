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
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.impl.metrics.UserMetricsUtil;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.GeneralStageWithKey;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

class StageWithGroupingBase<T, K> {

    final ComputeStageImplBase<T> computeStage;
    private final FunctionEx<? super T, ? extends K> keyFn;

    StageWithGroupingBase(
            @Nonnull ComputeStageImplBase<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        checkSerializable(keyFn, "keyFn");
        this.computeStage = computeStage;
        this.keyFn = keyFn;
    }

    @Nonnull
    public FunctionEx<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Nonnull
    <S, R, RET> RET attachMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        return computeStage.attachMapStateful(ttl, keyFn(), createFn, mapFn, onEvictFn);
    }

    @Nonnull
    <S, R, RET> RET attachFlatMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        return computeStage.attachFlatMapStateful(ttl, keyFn(), createFn, flatMapFn, onEvictFn);
    }

    @Nonnull
    <C, R, RET> RET attachMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        BiFunctionEx<C, T, ? extends R> biFunction = (c, t) -> {
            K k = keyFn.apply(t);
            return mapFn.apply(c, k, t);
        };
        return computeStage.attachMapUsingPartitionedContext(contextFactory, keyFn,
                UserMetricsUtil.wrap(biFunction, mapFn));
    }

    @Nonnull
    <C, RET> RET attachFilterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriPredicate<? super C, ? super K, ? super T> filterFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        BiPredicateEx<C, T> filterPredicate = (c, t) -> {
            K k = keyFn.apply(t);
            return filterFn.test(c, k, t);
        };
        return computeStage.attachFilterUsingPartitionedContext(contextFactory, keyFn,
                UserMetricsUtil.wrap(filterPredicate, filterFn));
    }

    @Nonnull
    <C, R, RET> RET attachFlatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachFlatMapUsingPartitionedContext(contextFactory, keyFn, (c, t) -> {
            K k = keyFn.apply(t);
            return flatMapFn.apply(c, k, t);
        });
    }

    @Nonnull
    <C, R, RET> RET attachTransformUsingContextAsync(
            @Nonnull String operationName,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        BiFunctionEx<C, T, CompletableFuture<Traverser<R>>> biFunction = (c, t) -> {
            K k = keyFn.apply(t);
            return flatMapAsyncFn.apply(c, k, t);
        };
        return computeStage.attachTransformUsingPartitionedContextAsync(operationName, contextFactory, keyFn,
                UserMetricsUtil.wrap(biFunction, flatMapAsyncFn));
    }

    static Transform transformOf(GeneralStageWithKey stage) {
        return ((StageWithGroupingBase) stage).computeStage.transform;
    }
}
