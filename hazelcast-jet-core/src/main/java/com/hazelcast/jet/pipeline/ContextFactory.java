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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * TODO add javadoc
 *
 * @param <C> context object type
 */
public final class ContextFactory<C> {

    private final DistributedFunction<? super Processor.Context, ? extends C> createFn;
    private final DistributedConsumer<? super C> destroyFn;

    private ContextFactory(
            DistributedFunction<? super Processor.Context, ? extends C> createFn,
            DistributedConsumer<? super C> destroyFn
    ) {
        this.createFn = createFn;
        this.destroyFn = destroyFn;
    }

    @Nonnull
    public static <C> ContextFactory<C> contextFactory(
            @Nonnull DistributedSupplier<? extends C> createContextFn
    ) {
        return new ContextFactory<>(procCtx -> createContextFn.get(), noopConsumer());
    }

    @Nonnull
    public static <C> ContextFactory<C> contextFactory(
            @Nonnull DistributedFunction<? super Processor.Context, ? extends C> createContextFn
    ) {
        return new ContextFactory<>(createContextFn, noopConsumer());
    }

    @Nonnull
    public ContextFactory<C> withDestroyFn(@Nonnull DistributedConsumer<? super C> destroyFn) {
        return new ContextFactory<>(createFn, destroyFn);
    }

    @Nonnull
    public DistributedFunction<? super Processor.Context, ? extends C> getCreateFn() {
        return createFn;
    }

    @Nonnull
    public DistributedConsumer<? super C> getDestroyFn() {
        return destroyFn;
    }
}
