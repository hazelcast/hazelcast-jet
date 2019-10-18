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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.processor.Processors.filterUsingContextP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextAsyncP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextP;
import static com.hazelcast.jet.core.processor.Processors.mapUsingContextP;

public final class PartitionedProcessorTransform<T, K> extends ProcessorTransform {

    private final FunctionEx<? super T, ? extends K> partitionKeyFn;

    private PartitionedProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        super(name, upstream, processorSupplier);
        this.partitionKeyFn = partitionKeyFn;
    }

    public static <T, K> PartitionedProcessorTransform<T, K> partitionedCustomProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>(name, upstream, processorSupplier, partitionKeyFn);
    }

    public static <C, T, K, R> PartitionedProcessorTransform<T, K> mapUsingContextPartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends R> mapFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("mapUsingPartitionedContext",
                upstream, ProcessorMetaSupplier.of(getPreferredLP(contextFactory),
                mapUsingContextP(contextFactory, mapFn)), partitionKeyFn);
    }

    public static <C, T, K> PartitionedProcessorTransform<T, K> filterUsingPartitionedContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiPredicateEx<? super C, ? super T> filterFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("filterUsingPartitionedContext",
                upstream, ProcessorMetaSupplier.of(getPreferredLP(contextFactory),
                filterUsingContextP(contextFactory, filterFn)), partitionKeyFn);
    }

    public static <C, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingPartitionedContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("flatMapUsingPartitionedContext",
                upstream, ProcessorMetaSupplier.of(getPreferredLP(contextFactory),
                flatMapUsingContextP(contextFactory, flatMapFn)), partitionKeyFn);
    }

    public static <C, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingPartitionedContextAsyncTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>(operationName + "UsingPartitionedContextAsync", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(contextFactory),
                        flatMapUsingContextAsyncP(contextFactory, partitionKeyFn, flatMapAsyncFn)), partitionKeyFn);
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, name(), localParallelism(), processorSupplier);
        p.addEdges(this, pv.v, e -> e.partitioned(partitionKeyFn).distributed());
    }
}
