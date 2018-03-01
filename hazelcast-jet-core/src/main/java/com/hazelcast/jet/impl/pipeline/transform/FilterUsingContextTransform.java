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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.TransformContext;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.filterUsingContextP;

public class FilterUsingContextTransform<C, T> extends AbstractTransform {
    private final TransformContext<C> transformContext;
    private final DistributedBiPredicate<C, T> filterFn;

    public FilterUsingContextTransform(
            @Nonnull Transform upstream,
            @Nonnull TransformContext<C> transformContext,
            @Nonnull DistributedBiPredicate<C, T> filterFn
    ) {
        super("filter", upstream);
        this.transformContext = transformContext;
        this.filterFn = filterFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                filterUsingContextP(transformContext, filterFn));
        p.addEdges(this, pv.v);
    }
}
