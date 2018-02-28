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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Factory to create and destroy context used in {@link
 * GeneralStage#useContext}.
 * <p>
 * Create new instances with static method {@link #contextFactory}.
 *
 * @param <C> context object type
 */
public final class ContextFactory<C> {

    private final DistributedFunction<? super JetInstance, ? extends C> createFn;
    private final DistributedConsumer<? super C> destroyFn;

    private ContextFactory(
            DistributedFunction<? super JetInstance, ? extends C> createFn,
            DistributedConsumer<? super C> destroyFn
    ) {
        this.createFn = createFn;
        this.destroyFn = destroyFn;
    }

    /**
     * Creates an immutable instance of {@link ContextFactory} that will create
     * contexts with the given {@code createContextFn}. If you need to clean-up
     * the context when the job shuts down, use {@link #withDestroyFn}.
     *
     * @param createContextFn function to create context object, given a {@link JetInstance}.
     *                        One context will be created for each parallel processor instance. The
     *                        returned context object will be used only in single thread, it doesn't
     *                        have to be thread-safe.
     * @param <C> context object type
     * @return the context factory
     */
    @Nonnull
    public static <C> ContextFactory<C> contextFactory(
            @Nonnull DistributedFunction<? super JetInstance, ? extends C> createContextFn
    ) {
        return new ContextFactory<>(createContextFn, noopConsumer());
    }

    /**
     * Returns a copy of this context factory with the destroy function replaced.
     *
     * @param destroyFn function to destroy the context instance. Will be called
     *                  when the job ends.
     */
    @Nonnull
    public ContextFactory<C> withDestroyFn(@Nonnull DistributedConsumer<? super C> destroyFn) {
        return new ContextFactory<>(createFn, destroyFn);
    }

    /**
     * Returns the function to create context.
     */
    @Nonnull
    public DistributedFunction<? super JetInstance, ? extends C> getCreateFn() {
        return createFn;
    }

    /**
     * Returns the function to destroy the context.
     */
    @Nonnull
    public DistributedConsumer<? super C> getDestroyFn() {
        return destroyFn;
    }
}
