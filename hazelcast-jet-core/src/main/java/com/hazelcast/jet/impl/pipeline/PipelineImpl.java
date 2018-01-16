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
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.SourceWithTimestamp;
import com.hazelcast.jet.pipeline.Stage;
import com.hazelcast.jet.core.DAG;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineImpl implements Pipeline {

    private final Map<Stage, List<Stage>> adjacencyMap = new HashMap<>();

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BatchStage<T> drawFrom(@Nonnull Source<? extends T> source) {
        return new BatchStageImpl<>((SourceTransform<? extends T>) source, this);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> StreamStage<T> drawFrom(@Nonnull SourceWithTimestamp<? extends T> source) {
        return new StreamStageImpl<>((SourceWithTimestampImpl<? extends T>) source, this);
    }

    @Nonnull @Override
    public DAG toDag() {
        return new Planner(this).createDag();
    }

    public void connect(GeneralStage upstream, Stage downstream) {
        adjacencyMap.get(upstream).add(downstream);
    }

    public void connect(List<GeneralStage> upstream, Stage downstream) {
        upstream.forEach(u -> connect(u, downstream));
    }

    <T> SinkStage drainTo(GeneralStage<? extends T> upstream, Sink<T> sink) {
        SinkStageImpl output = new SinkStageImpl(upstream, (SinkTransform) sink, this);
        connect(upstream, output);
        return output;
    }

    Map<Stage, List<Stage>> adjacencyMap() {
        Map<Stage, List<Stage>> safeCopy = new HashMap<>();
        adjacencyMap.forEach((k, v) -> safeCopy.put(k, new ArrayList<>(v)));
        return safeCopy;
    }

    void register(Stage stage, List<Stage> downstream) {
        List<Stage> prev = adjacencyMap.put(stage, downstream);
        assert prev == null : "Double registering of a Stage with this Pipeline: " + stage;
    }
}
