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
import com.hazelcast.jet.pipeline.PElement;
import com.hazelcast.jet.pipeline.impl.processor.CoGroupP;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.transform.MapTransform;
import com.hazelcast.jet.pipeline.impl.transform.PTransform;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.processor.Sources;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;

public class Planner {

    private final PipelineImpl pipeline;
    private final DAG dag = new DAG();
    private final Map<PElement, PlannerVertex> pel2vertex = new HashMap<>();


    public Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    @SuppressWarnings("unchecked")
    DAG createDag() {
        Iterable<AbstractPElement> sorted = topologicalSort(pipeline.adjacencyMap, Object::toString);
        System.out.println(sorted);
        for (AbstractPElement pel : sorted) {
            PTransform transform = pel.transform;
            if (transform instanceof SourceImpl) {
                SourceImpl source = (SourceImpl) transform;
                addVertex(pel, new Vertex("source." + source.name(), Sources.readMap(source.name())));
            } else if (transform instanceof MapTransform) {
                MapTransform mapTransform = (MapTransform) transform;
                PlannerVertex pv = addVertex(pel,
                        new Vertex("map." + randomSuffix(), Processors.map(mapTransform.mapF)));
                addEdge(pel.upstream.get(0), pv);
            } else if (transform instanceof CoGroupTransform) {
                CoGroupTransform coGroup = (CoGroupTransform) transform;
                PlannerVertex pv = addVertex(pel,
                        new Vertex("co-group." + randomSuffix(),
                                () -> new CoGroupP<>(coGroup.groupKeyFns(), coGroup.aggregateOperation(), coGroup.tags())));
                for (PElement fromPel : pel.upstream) {
                    addEdge(fromPel, pv);
                }
            } else if (transform instanceof SinkImpl) {
                SinkImpl sink = (SinkImpl) transform;
                PlannerVertex pv = addVertex(pel, new Vertex("sink." + sink.name(), Sinks.writeMap(sink.name())));
                addEdge(pel.upstream.get(0), pv);
            }
        }
        return dag;
    }

    private PlannerVertex addVertex(PElement pel, Vertex v) {
        dag.vertex(v);
        PlannerVertex pv = new PlannerVertex(v);
        pel2vertex.put(pel, pv);
        return pv;
    }

    private Edge addEdge(PElement fromPel, PlannerVertex toPv) {
        PlannerVertex fromPv = pel2vertex.get(fromPel);
        Edge edge = from(fromPv.v, fromPv.availableOrdinal++).to(toPv.v, toPv.availableOrdinal++);
        dag.edge(edge);
        return edge;
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
    }
}
