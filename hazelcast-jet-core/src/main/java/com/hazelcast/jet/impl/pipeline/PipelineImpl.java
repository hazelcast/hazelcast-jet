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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.SourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.core.DAG;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineImpl implements Pipeline {

    private final Map<Transform, List<Transform>> adjacencyMap = new HashMap<>();

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BatchStage<T> drawFrom(@Nonnull Source<? extends T> source) {
        return new BatchStageImpl<>((SourceTransform<? extends T>) source, this);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> StreamStage<T> drawFrom(@Nonnull StreamSource<? extends T> source) {
        return new StreamStageImpl<>((StreamSourceTransform<? extends T>) source, this);
    }

    @Nonnull @Override
    public DAG toDag() {
        return new Planner(this).createDag();
    }

    public void connect(Transform upstream, Transform downstream) {
        adjacencyMap.get(upstream).add(downstream);
    }

    public void connect(List<Transform> upstream, Transform downstream) {
        upstream.forEach(u -> connect(u, downstream));
    }

    <T> SinkStage drain(Transform upstream, Sink<T> sink) {
        SinkStageImpl output = new SinkStageImpl((SinkTransform) sink, this);
        connect(upstream, (SinkTransform) sink);
        return output;
    }

    Map<Transform, List<Transform>> adjacencyMap() {
        Map<Transform, List<Transform>> safeCopy = new HashMap<>();
        adjacencyMap.forEach((k, v) -> safeCopy.put(k, new ArrayList<>(v)));
        return safeCopy;
    }

    void register(Transform stage, List<Transform> downstream) {
        List<Transform> prev = adjacencyMap.put(stage, downstream);
        assert prev == null : "Double registering of a Stage with this Pipeline: " + stage;
    }
}
