/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.flatMapP;

public class FlatMapTransform<T, R> extends AbstractTransform {
    @Nonnull
    private DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn;

    public FlatMapTransform(
            @Nonnull Transform upstream,
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        super("flat-map", upstream);
        this.flatMapFn = flatMapFn;
    }

    @Nonnull
    public DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn() {
        return flatMapFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(), flatMapP(flatMapFn()));
        p.addEdges(this, pv.v);
    }
}
