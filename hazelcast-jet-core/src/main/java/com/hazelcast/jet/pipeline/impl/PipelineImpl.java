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
import com.hazelcast.jet.pipeline.PCollection;
import com.hazelcast.jet.pipeline.PElement;
import com.hazelcast.jet.pipeline.PEnd;
import com.hazelcast.jet.pipeline.PTransform;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.Transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineImpl implements Pipeline {

    private Map<PElement, List<PTransform>> inputMap = new HashMap<>();
    private Map<PTransform, List<PElement>> outputMap = new HashMap<>();

    public PipelineImpl() {
    }

    @Override
    public <E> PCollection<E> drawFrom(Source<E> source) {
        PCollectionImpl<E> output = new PCollectionImpl<>(this);
        addOutput(source, output);
        return output;
    }

    @Override
    public void execute(JetInstance jet) {
        printDAG();
    }

    <IN, OUT> PCollection<OUT> apply(PCollectionImpl<IN> input, Transform<IN, OUT> transform) {
        PCollectionImpl<OUT> output = new PCollectionImpl<>(this);
        addInput(input, transform);
        addOutput(transform, output);
        return output;
    }

    <E> PEnd drainTo(PCollectionImpl<E> input, Sink sink) {
        PEndImpl pEnd = new PEndImpl(this);
        addInput(input, sink);
        return pEnd;
    }

    //TODO: ordinal
    private void addInput(PElement input, PTransform transform) {
        inputMap.computeIfAbsent(input, e -> new ArrayList<>()).add(transform);
    }

    //TODO: ordinal
    private void addOutput(PTransform transform, PElement output) {
        outputMap.computeIfAbsent(transform, e -> new ArrayList<>()).add(output);
    }


    private void printDAG() {
        for (Entry<PTransform, List<PElement>> entry : outputMap.entrySet()) {
            Set<PTransform> outputs = entry.getValue().stream()
                                          .flatMap(e -> inputMap.get(e).stream()).collect(Collectors.toSet());
            System.out.println(entry.getKey() + " -> " + outputs);
        }
    }

}
