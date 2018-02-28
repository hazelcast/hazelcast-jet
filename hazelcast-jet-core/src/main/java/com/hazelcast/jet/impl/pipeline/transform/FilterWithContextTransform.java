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

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.filterWithContextP;

public class FilterWithContextTransform<C, T> extends AbstractTransform {
    private final DistributedFunction<? super JetInstance, ? extends C> createContextFn;
    private final DistributedBiPredicate<? super C, ? super T> filterFn;
    private final DistributedConsumer<? super C> destroyContextFn;

    public FilterWithContextTransform(
            @Nonnull Transform upstream,
            @Nonnull DistributedFunction<? super JetInstance, ? extends C> createContextFn,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn,
            @Nonnull DistributedConsumer<? super C> destroyContextFn
    ) {
        super("filter", upstream);
        this.createContextFn = createContextFn;
        this.filterFn = filterFn;
        this.destroyContextFn = destroyContextFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                filterWithContextP(createContextFn, filterFn, destroyContextFn));
        p.addEdges(this, pv.v);
    }
}
