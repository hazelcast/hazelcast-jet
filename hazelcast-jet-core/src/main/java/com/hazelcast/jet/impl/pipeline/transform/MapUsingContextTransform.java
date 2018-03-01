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

import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.mapUsingContextP;

public class MapUsingContextTransform<C, T, R> extends AbstractTransform {
    private final DistributedFunction<Context, ? extends C> createContextFn;
    private final DistributedBiFunction<C, ? super T, R> mapFn;
    private final DistributedConsumer<? super C> destroyContextFn;

    public MapUsingContextTransform(
            @Nonnull Transform upstream,
            @Nonnull DistributedFunction<Context, ? extends C> createContextFn,
            @Nonnull DistributedBiFunction<C, ? super T, R> mapFn,
            @Nonnull DistributedConsumer<? super C> destroyContextFn
    ) {
        super("map", upstream);
        this.createContextFn = createContextFn;
        this.mapFn = mapFn;
        this.destroyContextFn = destroyContextFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                mapUsingContextP(createContextFn, mapFn, destroyContextFn));
        p.addEdges(this, pv.v);
    }
}
