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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.PEnd;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.impl.transform.JoinTransform;
import com.hazelcast.jet.pipeline.impl.transform.PTransform;
import com.hazelcast.jet.pipeline.impl.transform.UnaryTransform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class PipelineImpl implements Pipeline {

    private Map<AbstractPElement, List<AbstractPElement>> outgoingEdges = new HashMap<>();

    public PipelineImpl() {
    }

    @Override
    public <E> PStream<E> drawFrom(Source<E> source) {
        return new PStreamImpl<>(emptyList(), source, this);
    }

    @Override
    public void execute(JetInstance jet) {
        printDAG();
    }

    public <IN, OUT> PStream<OUT> transform(PStreamImpl<IN> input, UnaryTransform<? super IN, OUT> unaryTransform) {
        PStreamImpl<OUT> output = new PStreamImpl<>(input, unaryTransform, this);
        addEdge(input, output);
        return output;
    }

    public PStream join(List<PStream> upstream, JoinTransform joinTransform) {
        PStreamImpl joined = new PStreamImpl(upstream, joinTransform, this);
        upstream.forEach(u -> addEdge((PStreamImpl) u, joined));
        return joined;
    }

    public <E> PEnd drainTo(PStreamImpl<E> input, Sink sink) {
        PEndImpl output = new PEndImpl(input, sink, this);
        addEdge(input, output);
        return output;
    }

    private void addEdge(AbstractPElement source, AbstractPElement dest) {
        outgoingEdges.computeIfAbsent(source, e -> new ArrayList<>()).add(dest);
    }


    private void printDAG() {
        for (Entry<AbstractPElement, List<AbstractPElement>> entry : outgoingEdges.entrySet()) {
            Set<PTransform> outputs = entry.getValue().stream()
                                           .map(e -> e.transform).collect(Collectors.toSet());
            System.out.println(entry.getKey().transform + " -> " + outputs);
        }
    }


}
