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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.stream.impl.processor.SortP;

import java.util.Comparator;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;

class SortPipe<T> extends AbstractIntermediatePipe<T, T> {

    private final Comparator<? super T> comparator;

    SortPipe(Pipe<T> upstream, StreamContext context, Comparator<? super T> comparator) {
        super(context, true, upstream);
        this.comparator = comparator;
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        Vertex previous = upstream.buildDAG(dag);
        // required final for lambda variable capture
        final Comparator<? super T> comparator = this.comparator;
        Vertex sorter = dag.newVertex(uniqueVertexName("sort"), () -> new SortP<>(comparator)).localParallelism(1);
        dag.edge(between(previous, sorter)
                .distributed()
                .allToOne()
        );

        return sorter;
    }
}
