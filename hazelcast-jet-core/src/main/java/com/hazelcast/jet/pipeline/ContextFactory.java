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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Holder of functions to create and destroy context for transform functions.
 * See:<ul>
 *     <li>{@link GeneralStage#mapUsingContext}
 *     <li>{@link GeneralStage#filterUsingContext}
 *     <li>{@link GeneralStage#flatMapUsingContext}
 * </ul>
 *
 * Create new instances using {@link #withCreateFn} or choose one of the
 * predefined instances in {@link ContextFactories}. Instances are immutable.
 *
 * @param <C> the user-defined context object type
 */
public final class ContextFactory<C> implements Serializable {

    private static final boolean COOPERATIVE_DEFAULT = true;
    private static final boolean SHARE_LOCALLY_DEFAULT = false;

    private final DistributedFunction<JetInstance, ? extends C> createFn;
    private final DistributedConsumer<? super C> destroyFn;
    private final boolean cooperative;
    private final boolean shareLocally;

    private ContextFactory(
            DistributedFunction<JetInstance, ? extends C> createFn,
            DistributedConsumer<? super C> destroyFn,
            boolean cooperative, boolean shareLocally) {
        this.createFn = createFn;
        this.destroyFn = destroyFn;
        this.cooperative = cooperative;
        this.shareLocally = shareLocally;
    }

    /**
     * Creates new {@link ContextFactory} with given create function.
     *
     * @param createContextFn the function to create new context object, given a JetInstance
     * @param <C> the user-defined context object type
     * @return a new instance
     */
    @Nonnull
    public static <C> ContextFactory<C> withCreateFn(
            @Nonnull DistributedFunction<JetInstance, ? extends C> createContextFn
    ) {
        return new ContextFactory<>(createContextFn, noopConsumer(), COOPERATIVE_DEFAULT, SHARE_LOCALLY_DEFAULT);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with destroy function
     * replaced with the given function.
     *
     * @param destroyFn the function to destroy user-defined context. It will be called
     *                  when the job finishes
     * @return a new instance with destroy function replaced
     */
    @Nonnull
    public ContextFactory<C> withDestroyFn(@Nonnull DistributedConsumer<? super C> destroyFn) {
        return new ContextFactory<>(createFn, destroyFn, cooperative, shareLocally);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with <em>cooperative</em>
     * flag set to {@code false}. Context factory is cooperative by default.
     * Call this method if the calls to context are non-cooperative.
     * <p>
     * The contract of <em>cooperative processing</em> is described {@link
     * Processor#isCooperative() here}.
     *
     * @return a new instance with <em>cooperative</em> flag un-set.
     */
    @Nonnull
    public ContextFactory<C> nonCooperative() {
        return new ContextFactory<>(createFn, destroyFn, false, shareLocally);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with <em>share-locally</em>
     * flag set to {@code true}. By default, context object is not shared, each
     * parallel processor gets its own instance. If the context object is large
     * or takes long to initialize, it might be better to share it. If you
     * enable sharing, the context object will be used from multiple threads -
     * make sure that it is  <u>thread-safe</u>.
     *
     * @return a new instance with <em>share-locally</em> flag set.
     */
    @Nonnull
    public ContextFactory<C> shareLocally() {
        return new ContextFactory<>(createFn, destroyFn, cooperative, true);
    }

    /**
     * Returns the create function.
     */
    @Nonnull
    public DistributedFunction<JetInstance, ? extends C> createFn() {
        return createFn;
    }

    /**
     * Returns the destroy function.
     */
    @Nonnull
    public DistributedConsumer<? super C> destroyFn() {
        return destroyFn;
    }

    /**
     * Returns the cooperative flag.
     */
    public boolean isCooperative() {
        return cooperative;
    }

    /**
     * Returns the share-locally flag.
     */
    public boolean isSharedLocally() {
        return shareLocally;
    }
}
