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

import java.io.Serializable;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * TODO add javadoc
 *
 * @param <C> context object type
 */
public final class TransformContext<C> implements Serializable {

    private final DistributedFunction<JetInstance, ? extends C> createFn;
    private final DistributedConsumer<? super C> destroyFn;

    private TransformContext(
            DistributedFunction<JetInstance, ? extends C> createFn,
            DistributedConsumer<? super C> destroyFn
    ) {
        this.createFn = createFn;
        this.destroyFn = destroyFn;
    }

    @Nonnull
    public static <C> TransformContext<C> withCreate(
            @Nonnull DistributedFunction<JetInstance, ? extends C> createContextFn
    ) {
        return new TransformContext<>(createContextFn, noopConsumer());
    }

    @Nonnull
    public TransformContext<C> withDestroy(@Nonnull DistributedConsumer<? super C> destroyFn) {
        return new TransformContext<>(createFn, destroyFn);
    }

    @Nonnull
    public DistributedFunction<JetInstance, ? extends C> getCreateFn() {
        return createFn;
    }

    @Nonnull
    public DistributedConsumer<? super C> getDestroyFn() {
        return destroyFn;
    }
}
