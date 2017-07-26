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
import com.hazelcast.jet.pipeline.impl.processor.CoGroupP;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.transform.MapTransform;
import com.hazelcast.jet.pipeline.impl.transform.PTransform;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.processor.Sources;

import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;

public class Planner {

    private PipelineImpl pipeline;

    public Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    @SuppressWarnings("unchecked")
    DAG createDag() {
        Iterable<AbstractPElement> sorted = topologicalSort(pipeline.adjacencyMap, Object::toString);
        System.out.println(sorted);
        DAG dag = new DAG();
        for (AbstractPElement pel : sorted) {
            PTransform transform = pel.getTransform();
            if (transform instanceof SourceImpl) {
                SourceImpl source = (SourceImpl) transform;
                dag.newVertex("source", Sources.readMap(source.name()));
            } else if (transform instanceof MapTransform) {
                MapTransform mapTransform = (MapTransform) transform;
                dag.newVertex("map", Processors.map(mapTransform.mapF));
            } else if (transform instanceof CoGroupTransform) {
                CoGroupTransform coGroup = (CoGroupTransform) transform;
                dag.newVertex("co-group", () -> new CoGroupP<>(
                        coGroup.groupKeyFns(), coGroup.aggregateOperation(), coGroup.tags()));
            } else if (transform instanceof SinkImpl) {
                SinkImpl sink = (SinkImpl) transform;
                dag.newVertex("sink", Sinks.writeMap(sink.name()));
            }
        }
        return dag;
    }
}
