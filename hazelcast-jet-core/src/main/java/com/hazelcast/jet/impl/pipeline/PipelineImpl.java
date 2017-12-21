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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.SinkStage;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Stage;
import com.hazelcast.jet.impl.pipeline.transform.MultaryTransform;
import com.hazelcast.jet.impl.pipeline.transform.UnaryTransform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineImpl implements Pipeline {

    private final Map<Stage, List<Stage>> adjacencyMap = new HashMap<>();

    @Nonnull @Override
    public <T> ComputeStage<T> drawFrom(@Nonnull Source<? extends T> source) {
        return new ComputeStageImpl<>(source, this);
    }

    @Nonnull @Override
    public DAG toDag() {
        return new Planner(this).createDag();
    }

    public <R> ComputeStage<R> attach(MultaryTransform<? extends R> transform, List<ComputeStage> upstream) {
        ComputeStageImpl<R> attached = new ComputeStageImpl<>(upstream, transform, this);
        upstream.forEach(u -> connect(u, attached));
        return attached;
    }

    <T, R> ComputeStage<R> attach(
            @Nonnull UnaryTransform<? super T, ? extends R> unaryTransform,
            @Nullable ComputeStage<T> upstream
    ) {
        ComputeStageImpl<R> attached = new ComputeStageImpl<>(upstream, unaryTransform, this);
        connect(upstream, attached);
        return attached;
    }

    <T> SinkStage drainTo(ComputeStage<? extends T> upstream, Sink<T> sink) {
        SinkStageImpl output = new SinkStageImpl(upstream, sink, this);
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

    private void connect(ComputeStage upstream, Stage downstream) {
        adjacencyMap.get(upstream).add(downstream);
    }
}
