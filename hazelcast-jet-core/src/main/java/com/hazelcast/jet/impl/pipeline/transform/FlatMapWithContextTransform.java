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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.flatMapWithContextP;

public class FlatMapWithContextTransform<C, T, R> extends AbstractTransform {
    @Nonnull private final DistributedFunction<? super Context, ? extends C> createContextFn;
    @Nonnull
    private DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn;
    @Nonnull private final DistributedConsumer<? super C> destroyContextFn;

    public FlatMapWithContextTransform(
            @Nonnull Transform upstream,
            @Nonnull DistributedFunction<? super Context, ? extends C> createContextFn,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn,
            @Nonnull DistributedConsumer<? super C> destroyContextFn
    ) {
        super("flat-map", upstream);
        this.createContextFn = createContextFn;
        this.flatMapFn = flatMapFn;
        this.destroyContextFn = destroyContextFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                flatMapWithContextP(createContextFn, flatMapFn, destroyContextFn));
        p.addEdges(this, pv.v);
    }
}
