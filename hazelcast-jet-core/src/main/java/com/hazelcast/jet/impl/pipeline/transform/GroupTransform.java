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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.accumulateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;

public class GroupTransform<K, A, R, OUT> extends AbstractTransform {
    @Nonnull
    private List<DistributedFunction<?, ? extends K>> groupKeyFns;
    @Nonnull
    private AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn;

    public GroupTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull List<DistributedFunction<?, ? extends K>> groupKeyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        super(upstream.size() + "-way cogroup-and-aggregate", upstream);
        this.groupKeyFns = groupKeyFns;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    //                   ---------        ---------
    //                  | source0 |  ... | sourceN |
    //                   ---------        ---------
    //                       |                  |
    //                  partitioned        partitioned
    //                       v                  v
    //                       --------------------
    //                      |  accumulateByKeyP  |
    //                       --------------------
    //                                 |
    //                            distributed
    //                            partitioned
    //                                 v
    //                          ---------------
    //                         | combineByKeyP |
    //                          ---------------
    @Override
    public void addToDag(Planner p) {
        List<DistributedFunction<?, ? extends K>> groupKeyFns = this.groupKeyFns;
        String namePrefix = p.vertexName(this.name(), "-stage");
        Vertex v1 = p.dag.newVertex(namePrefix + '1', accumulateByKeyP(groupKeyFns, aggrOp))
                .localParallelism(getLocalParallelism());
        PlannerVertex pv2 = p.addVertex(this, namePrefix + '2', getLocalParallelism(),
                combineByKeyP(aggrOp, mapToOutputFn));
        p.addEdges(this, v1, (e, ord) -> e.partitioned(groupKeyFns.get(ord), HASH_CODE));
        p.dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }
}
