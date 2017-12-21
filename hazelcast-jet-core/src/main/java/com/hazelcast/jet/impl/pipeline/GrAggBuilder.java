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

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.StageWithGrouping;
import com.hazelcast.jet.StageWithGroupingAndTimestamp;
import com.hazelcast.jet.StageWithGroupingAndWindow;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.CoGroupTransform;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static java.util.stream.Collectors.toList;

/**
 * Support class for {@link com.hazelcast.jet.GroupAggregateBuilder}
 * and {@link com.hazelcast.jet.WindowGroupAggregateBuilder}. The
 * motivation is to have the ability to specify different output
 * types ({@code Entry<K, R>} vs. {@code TimestampedEntry<K, R>}).
 *
 * @param <K> type of the grouping key
 */
public class GrAggBuilder<K> {
    private final WindowDefinition wDef;
    private final List<StageWithGroupingBase<?, K>> stages = new ArrayList<>();
    private final List<DistributedToLongFunction<?>> timestampFns;

    @SuppressWarnings("unchecked")
    public GrAggBuilder(StageWithGrouping<?, K> s) {
        wDef = null;
        timestampFns = null;
        stages.add((StageWithGroupingBase<?, K>) s);
    }

    @SuppressWarnings("unchecked")
    public GrAggBuilder(StageWithGroupingAndWindow<?, K> s) {
        wDef = s.windowDefinition();
        stages.add((StageWithGroupingBase<?, K>) s);
        timestampFns = new ArrayList<>();
        timestampFns.add(s.timestampFn());
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(StageWithGroupingAndTimestamp<E, K> stage) {
        stages.add((StageWithGroupingBase<E, K>) stage);
        timestampFns.add(stage.timestampFn());
        return (Tag<E>) tag(stages.size() - 1);
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(StageWithGrouping<E, K> stage) {
        stages.add((StageWithGroupingBase<E, K>) stage);
        return (Tag<E>) tag(stages.size() - 1);
    }

    public <A, R, OUT> ComputeStage<OUT> build(AggregateOperation<A, R> aggrOp) {
        CoGroupTransform<K, A, R, OUT> transform = new CoGroupTransform<>(
                stages.stream().map(StageWithGroupingBase::keyFn).collect(toList()),
                aggrOp, timestampFns,
                wDef
        );
        PipelineImpl pipeline = (PipelineImpl) stages.get(0).computeStage().getPipeline();
        return pipeline.attach(
                transform, stages.stream()
                                 .map(StageWithGroupingBase::computeStage)
                                 .collect(toList())
        );
    }
}
