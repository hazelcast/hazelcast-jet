/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.processor.HashJoinCollectP;
import com.hazelcast.jet.impl.processor.HashJoinP;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.impl.pipeline.Planner.tailList;
import static com.hazelcast.jet.impl.util.Util.toList;

public class HashJoinTransform<T0, R> extends AbstractTransform {
    @Nonnull
    private final List<JoinClause<?, ? super T0, ?, ?>> clauses;
    @Nonnull
    private final List<Tag> tags;
    @Nullable
    private final BiFunctionEx mapToOutputBiFn;
    @Nullable
    private final TriFunction mapToOutputTriFn;

    public HashJoinTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull List<JoinClause<?, ? super T0, ?, ?>> clauses,
            @Nonnull List<Tag> tags,
            @Nonnull BiFunctionEx mapToOutputBiFn
    ) {
        super(upstream.size() + "-way hash-join", upstream);
        this.clauses = clauses;
        this.tags = tags;
        this.mapToOutputBiFn = mapToOutputBiFn;
        this.mapToOutputTriFn = null;
    }

    public <T1, T2> HashJoinTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull List<JoinClause<?, ? super T0, ?, ?>> clauses,
            @Nonnull List<Tag> tags,
            @Nonnull TriFunction<T0, T1, T2, R> mapToOutputTriFn
    ) {
        super(upstream.size() + "-way hash-join", upstream);
        this.clauses = clauses;
        this.tags = tags;
        this.mapToOutputBiFn = null;
        this.mapToOutputTriFn = mapToOutputTriFn;
    }

    //         ---------           ----------           ----------
    //        | primary |         | joined-1 |         | joined-2 |
    //         ---------           ----------           ----------
    //             |                   |                     |
    //             |              distributed          distributed
    //             |               broadcast            broadcast
    //             |                   v                     v
    //             |             -------------         -------------
    //             |            | collector-1 |       | collector-2 |
    //             |             -------------         -------------
    //             |                   |                     |
    //             |                 local                 local
    //           local             broadcast             broadcast
    //          unicast           prioritized           prioritized
    //         ordinal 0           ordinal 1             ordinal 2
    //             \                   |                     |
    //              ----------------\  |   /----------------/
    //                              v  v  v
    //                              --------
    //                             | joiner |
    //                              --------
    @Override
    @SuppressWarnings("unchecked")
    public void addToDag(Planner p) {
        PlannerVertex primary = p.xform2vertex.get(this.upstream().get(0));
        List keyFns = toList(this.clauses, JoinClause::leftKeyFn);

        List<Tag> tags = this.tags;
        BiFunctionEx mapToOutputBiFn = this.mapToOutputBiFn;
        TriFunction mapToOutputTriFn = this.mapToOutputTriFn;
        Vertex joiner = p.addVertex(this, name() + "-joiner", localParallelism(),
                () -> new HashJoinP<>(keyFns, tags, mapToOutputBiFn, mapToOutputTriFn)).v;
        p.dag.edge(from(primary.v, primary.nextAvailableOrdinal()).to(joiner, 0));

        String collectorName = name() + "-collector";
        int collectorOrdinal = 1;
        for (Transform fromTransform : tailList(this.upstream())) {
            PlannerVertex fromPv = p.xform2vertex.get(fromTransform);
            JoinClause<?, ?, ?, ?> clause = this.clauses.get(collectorOrdinal - 1);
            FunctionEx<Object, Object> getKeyFn =
                    (FunctionEx<Object, Object>) clause.rightKeyFn();
            FunctionEx<Object, Object> projectFn =
                    (FunctionEx<Object, Object>) clause.rightProjectFn();
            Vertex collector = p.dag.newVertex(collectorName + collectorOrdinal,
                    () -> new HashJoinCollectP(getKeyFn, projectFn));
            collector.localParallelism(1);
            p.dag.edge(from(fromPv.v, fromPv.nextAvailableOrdinal())
                    .to(collector, 0)
                    .distributed().broadcast());
            p.dag.edge(from(collector, 0)
                    .to(joiner, collectorOrdinal)
                    .broadcast().priority(-1));
            collectorOrdinal++;
        }
    }
}
