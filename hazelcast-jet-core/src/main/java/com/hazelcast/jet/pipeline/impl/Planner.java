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

package com.hazelcast.jet.pipeline.impl;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.processor.HashJoinP;
import com.hazelcast.jet.pipeline.Stage;
import com.hazelcast.jet.pipeline.Transform;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.transform.FilterTransform;
import com.hazelcast.jet.pipeline.impl.transform.FlatMapTransform;
import com.hazelcast.jet.pipeline.impl.transform.GroupByTransform;
import com.hazelcast.jet.pipeline.impl.transform.HashJoinTransform;
import com.hazelcast.jet.pipeline.impl.transform.MapTransform;
import com.hazelcast.jet.pipeline.impl.transform.ProcessorTransform;
import com.hazelcast.jet.processor.Processors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;

class Planner {

    private final PipelineImpl pipeline;
    private final DAG dag = new DAG();
    private final Map<Stage, PlannerVertex> stage2vertex = new HashMap<>();


    Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    @SuppressWarnings("unchecked")
    DAG createDag() {
        Iterable<AbstractStage> sorted = (Iterable<AbstractStage>) (Iterable<? extends Stage>)
                topologicalSort(pipeline.adjacencyMap, Object::toString);
        for (AbstractStage stage : sorted) {
            Transform transform = stage.transform;
            if (transform instanceof SourceImpl) {
                SourceImpl source = (SourceImpl) transform;
                addVertex(stage, new Vertex(source.name(), source.metaSupplier())
                        .localParallelism(1));
            } else if (transform instanceof ProcessorTransform) {
                ProcessorTransform procTransform = (ProcessorTransform) transform;
                PlannerVertex pv = addVertex(stage,
                        new Vertex(procTransform.transformName + '.' + randomSuffix(), procTransform.procSupplier));
                addEdges(stage, pv.v);
            } else if (transform instanceof FilterTransform) {
                FilterTransform filter = (FilterTransform) transform;
                PlannerVertex pv = addVertex(stage,
                        new Vertex("filter." + randomSuffix(), Processors.filter(filter.filterF)));
                addEdges(stage, pv.v);
            } else if (transform instanceof MapTransform) {
                MapTransform map = (MapTransform) transform;
                PlannerVertex pv = addVertex(stage,
                        new Vertex("map." + randomSuffix(), Processors.map(map.mapF)));
                addEdges(stage, pv.v);
            } else if (transform instanceof FlatMapTransform) {
                FlatMapTransform flatMap = (FlatMapTransform) transform;
                PlannerVertex pv = addVertex(stage,
                        new Vertex("flatMap." + randomSuffix(), Processors.flatMap(flatMap.flatMapF())));
                addEdges(stage, pv.v);
            } else if (transform instanceof GroupByTransform) {
                GroupByTransform<Object, Object, Object> groupBy = (GroupByTransform) transform;
                String name = "groupByKey." + randomSuffix() + ".stage";
                Vertex v1 = dag.newVertex(name + '1',
                        Processors.accumulateByKey(groupBy.keyF(), groupBy.aggregateOperation()));
                PlannerVertex pv2 = addVertex(stage, new Vertex(name + '2',
                        Processors.combineByKey(groupBy.aggregateOperation())));
                addEdges(stage, v1, e -> e.partitioned(groupBy.keyF(), HASH_CODE));
                dag.edge(between(v1, pv2.v).distributed().partitioned(groupBy.keyF()));
            } else if (transform instanceof CoGroupTransform) {
                CoGroupTransform<Object, Object, Object> coGroup = (CoGroupTransform) transform;
                List<DistributedFunction<?, ?>> groupKeyFs = coGroup.groupKeyFs();
                String name = "coGroup." + randomSuffix() + ".stage";
                Vertex v1 = dag.newVertex(name + '1',
                        Processors.coAccumulateByKey(groupKeyFs, coGroup.aggregateOperation()));
                PlannerVertex pv2 = addVertex(stage, new Vertex(name + '2',
                        Processors.combineByKey(coGroup.aggregateOperation())));
                addEdges(stage, v1, (e, ord) -> e.partitioned(groupKeyFs.get(ord), HASH_CODE));
                dag.edge(between(v1, pv2.v).distributed().partitioned(Entry<Object, Object>::getKey));
            } else if (transform instanceof HashJoinTransform) {
                HashJoinTransform hashJoin = (HashJoinTransform) transform;
                PlannerVertex pv = addVertex(stage, new Vertex("hashJoin." + randomSuffix(),
                        () -> new HashJoinP(hashJoin.joinOns(), hashJoin.tags())));
                addEdges(stage, pv.v, (e, ordinal) -> {
                    if (ordinal > 0) {
                        e.distributed().broadcast().priority(-1);
                    }
                });
            } else if (transform instanceof SinkImpl) {
                SinkImpl sink = (SinkImpl) transform;
                PlannerVertex pv = addVertex(stage, new Vertex(sink.name(), sink.metaSupplier())
                        .localParallelism(1));
                addEdges(stage, pv.v);
            } else {
                throw new IllegalArgumentException("Unknown transform " + transform);
            }
        }
        return dag;
    }

    private PlannerVertex addVertex(Stage pel, Vertex v) {
        dag.vertex(v);
        PlannerVertex pv = new PlannerVertex(v);
        stage2vertex.put(pel, pv);
        return pv;
    }

    private void addEdges(AbstractStage stage, Vertex toVertex, BiConsumer<Edge, Integer> configureEdgeF) {
        int destOrdinal = 0;
        for (Stage fromStage : stage.upstream) {
            PlannerVertex fromPv = stage2vertex.get(fromStage);
            Edge edge = from(fromPv.v, fromPv.availableOrdinal++).to(toVertex, destOrdinal);
            dag.edge(edge);
            configureEdgeF.accept(edge, destOrdinal);
            destOrdinal++;
        }
    }

    private void addEdges(AbstractStage stage, Vertex toVertex, Consumer<Edge> configureEdgeF) {
        addEdges(stage, toVertex, (e, ord) -> configureEdgeF.accept(e));
    }

    private void addEdges(AbstractStage stage, Vertex toVertex) {
        addEdges(stage, toVertex, e -> {});
    }

    private static String randomSuffix() {
        String uuid = newUnsecureUUID().toString();
        return uuid.substring(uuid.length() - 8, uuid.length());
    }

    private static class PlannerVertex {
        Vertex v;

        int availableOrdinal;

        PlannerVertex(Vertex v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return v.toString();
        }
    }
}
