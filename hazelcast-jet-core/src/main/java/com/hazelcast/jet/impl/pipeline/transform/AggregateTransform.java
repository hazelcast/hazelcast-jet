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

import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.accumulateP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;

public class AggregateTransform<A, R> extends AbstractTransform implements Transform {
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;

    public AggregateTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        super(upstream.size() + "-way co-aggregate", upstream);
        this.aggrOp = aggrOp;
    }

    @Nonnull
    public AggregateOperation<A, R> aggrOp() {
        return aggrOp;
    }

    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //                   |              |
    //                   v              v
    //                  -------------------
    //                 | accumulatebyKeyP  |
    //                  -------------------
    //                           |
    //                      distributed
    //                       all-to-one
    //                           v
    //                   ----------------
    //                  | combineByKeyP  |
    //                   ----------------
    @Override
    public void addToDag(Planner p) {
        String namePrefix = p.vertexName(name(), "-stage");
        Vertex v1 = p.dag.newVertex(namePrefix + '1', accumulateP(aggrOp()));
        PlannerVertex pv2 = p.addVertex(this, namePrefix + '2', combineByKeyP(aggrOp(), Util::entry));
        p.addEdges(this, v1);
        p.dag.edge(between(v1, pv2.v).distributed().allToOne());
    }
}
