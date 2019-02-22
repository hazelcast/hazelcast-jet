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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.util.ConstantFunction;
import com.hazelcast.jet.impl.pipeline.SessionWindowDefinition;
import com.hazelcast.jet.impl.pipeline.SlidingWindowDefinition;
import com.hazelcast.jet.impl.pipeline.WindowDefinitionBase;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.impl.pipeline.transform.AbstractTransform.Optimization.MEMORY;
import static com.hazelcast.jet.impl.pipeline.transform.AggregateTransform.FIRST_STAGE_VERTEX_NAME_SUFFIX;
import static java.util.Collections.nCopies;

public class WindowAggregateTransform<A, R, OUT> extends AbstractTransform {
    @Nonnull
    private final AggregateOperation<A, ? extends R> aggrOp;
    @Nonnull
    private final WindowDefinitionBase wDef;
    @Nonnull
    private final WindowResultFunction<? super R, ? extends OUT> mapToOutputFn;

    public WindowAggregateTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull WindowDefinitionBase wDef,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        super(createName(wDef), upstream);
        this.aggrOp = aggrOp;
        this.wDef = wDef;
        this.mapToOutputFn = mapToOutputFn;
    }

    private static String createName(WindowDefinitionBase wDef) {
        return wDef.name() + "-window";
    }

    @Override
    public long preferredWatermarkStride() {
        return wDef.preferredWatermarkStride();
    }

    @Override
    public void addToDag(Planner p) {
        if (wDef instanceof SessionWindowDefinition) {
            addSessionWindow(p, (SessionWindowDefinition) wDef);
        } else if (aggrOp.combineFn() == null || wDef.earlyResultsPeriod() > 0 || getOptimization() == MEMORY) {
            addSlidingWindowSingleStage(p, (SlidingWindowDefinition) wDef);
        } else {
            addSlidingWindowTwoStage(p, (SlidingWindowDefinition) wDef);
        }
    }

    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //              distributed    distributed
    //              all-to-one      all-to-one
    //                   \              /
    //                    ---\    /-----
    //                        v  v
    //             ---------------------------
    //            | aggregateToSlidingWindowP | local parallelism = 1
    //             ---------------------------
    private void addSlidingWindowSingleStage(Planner p, SlidingWindowDefinition wDef) {
        PlannerVertex pv = p.addVertex(this, name(), 1,
                aggregateToSlidingWindowP(
                        nCopies(aggrOp.arity(), new ConstantFunction<>(name().hashCode())),
                        nCopies(aggrOp.arity(), (DistributedToLongFunction<JetEvent>) JetEvent::timestamp),
                        TimestampKind.EVENT,
                        wDef.toSlidingWindowPolicy(),
                        wDef.earlyResultsPeriod(),
                        aggrOp,
                        mapToOutputFn.toKeyedWindowResultFn()
                ));
        p.addEdges(this, pv.v, edge -> edge.distributed().allToOne(name().hashCode()));
    }

    //               --------        ---------
    //              | source0 | ... | sourceN |
    //               --------        ---------
    //                   |               |
    //                 local           local
    //                unicast         unicast
    //                   v               v
    //                  --------------------
    //                 | accumulateByFrameP | keyFn = constantKey()
    //                  --------------------
    //                           |
    //                      distributed
    //                       all-to-one
    //                           v
    //               -------------------------
    //              | combineToSlidingWindowP | local parallelism = 1
    //               -------------------------
    private void addSlidingWindowTwoStage(Planner p, SlidingWindowDefinition wDef) {
        SlidingWindowPolicy winPolicy = wDef.toSlidingWindowPolicy();
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, accumulateByFrameP(
                nCopies(aggrOp.arity(), new ConstantFunction<>(name().hashCode())),
                nCopies(aggrOp.arity(), (DistributedToLongFunction<JetEvent>) JetEvent::timestamp),
                TimestampKind.EVENT,
                winPolicy,
                aggrOp
        ));
        v1.localParallelism(localParallelism());
        PlannerVertex pv2 = p.addVertex(this, name(), 1,
                combineToSlidingWindowP(winPolicy, aggrOp, mapToOutputFn.toKeyedWindowResultFn()));
        p.addEdges(this, v1);
        p.dag.edge(between(v1, pv2.v).distributed().allToOne(name().hashCode()));
    }

    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //              distributed    distributed
    //              all-to-one      all-to-one
    //                   \              /
    //                    ---\    /-----
    //                        v  v
    //             ---------------------------
    //            | aggregateToSessionWindowP | local parallelism = 1
    //             ---------------------------
    private void addSessionWindow(Planner p, SessionWindowDefinition wDef) {
        PlannerVertex pv = p.addVertex(this, name(), localParallelism(),
                aggregateToSessionWindowP(
                        wDef.sessionTimeout(),
                        wDef.earlyResultsPeriod(),
                        nCopies(aggrOp.arity(), (DistributedToLongFunction<JetEvent>) JetEvent::timestamp),
                        nCopies(aggrOp.arity(), new ConstantFunction<>(name().hashCode())),
                        aggrOp,
                        mapToOutputFn.toKeyedWindowResultFn()));
        p.addEdges(this, pv.v, edge -> edge.distributed().allToOne(name().hashCode()));
    }
}
