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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.distinctP;

public class DistinctTransform<T, K> extends AbstractTransform {
    private final DistributedFunction<? super T, ? extends K> keyFn;

    public DistinctTransform(Transform upstream, DistributedFunction<? super T, ? extends K> keyFn) {
        super("distinct", upstream);
        this.keyFn = keyFn;
    }

    @Override
    public void addToDag(Planner p) {
        String namePrefix = p.uniqueVertexName(this.name(), "-step");
        Vertex v1 = p.dag.newVertex(namePrefix + '1', distinctP(keyFn))
                         .localParallelism(localParallelism());
        PlannerVertex pv2 = p.addVertex(this, namePrefix + '2', localParallelism(), distinctP(keyFn));
        p.addEdges(this, v1, (e, ord) -> e.partitioned(keyFn, HASH_CODE));
        p.dag.edge(between(v1, pv2.v).distributed().partitioned(keyFn));
    }
}
