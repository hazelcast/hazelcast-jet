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
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithContext;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;

public class StreamStageWithContextImpl<T, C> extends StageWithContextBase<T, C> implements StreamStageWithContext<T, C> {
    public StreamStageWithContextImpl(
            @Nonnull ComputeStageImplBase<T> computeStage,
            @Nonnull ContextFactory<C> contextFactory
    ) {
        super(computeStage, contextFactory);
    }

    @Nonnull
    @Override
    public StreamStage<T> filter(@Nonnull DistributedBiPredicate<? super C, ? super T> filterFn) {
        return computeStage.attachFilterWithContext(contextFactory, filterFn);
    }

    @Nonnull
    @Override
    public <R> StreamStage<R> map(@Nonnull DistributedBiFunction<? super C, ? super T, R> mapFn) {
        return computeStage.attachMapWithContext(contextFactory, mapFn);
    }

    @Nonnull
    @Override
    public <R> StreamStage<R> flatMap(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return computeStage.attachFlatMapWithContext(contextFactory, flatMapFn);
    }
}
