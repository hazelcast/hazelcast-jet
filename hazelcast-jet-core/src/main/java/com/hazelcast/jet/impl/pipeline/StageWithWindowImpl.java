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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowAggregateBuilder;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static java.util.Arrays.asList;

/**
 * Javadoc pending.
 */
public class StageWithWindowImpl<T> implements StageWithWindow<T> {

    private final StreamStageImpl<T> streamStage;
    private final WindowDefinition wDef;

    StageWithWindowImpl(StreamStageImpl<T> streamStage, WindowDefinition wDef) {
        this.streamStage = streamStage;
        this.wDef = wDef;
    }

    @Nonnull @Override
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Nonnull @Override
    public <K> StageWithGroupingAndWindow<T, K> groupingKey(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        return new StageWithGroupingAndWindowImpl<>(streamStage, keyFn, wDef);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <A, R> StreamStage<TimestampedEntry<Void, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        ensureJetEvents(streamStage, "This pipeline stage");
        AggregateOperation<?, ?> adaptedAggrOp = ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp);
        return streamStage.attach(new AggregateTransform<>(
                streamStage.transform, adaptedAggrOp, wDef
        ), ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, A, R> StreamStage<TimestampedEntry<Void, R>> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        ComputeStageImplBase stageImpl1 = (ComputeStageImplBase) stage1;
        ensureJetEvents(streamStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        AggregateOperation<?, ?> adaptedAggrOp = ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp);
        return streamStage.attach(new CoAggregateTransform<>(
                asList(streamStage.transform, stageImpl1.transform),
                adaptedAggrOp, wDef
        ), ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R> StreamStage<TimestampedEntry<Void, R>> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        ComputeStageImplBase stageImpl1 = (ComputeStageImplBase) stage1;
        ComputeStageImplBase stageImpl2 = (ComputeStageImplBase) stage2;
        ensureJetEvents(streamStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        AggregateOperation<?, ?> adaptedAggrOp = ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp);
        return streamStage.attach(new CoAggregateTransform<>(
                asList(streamStage.transform, stageImpl1.transform, stageImpl2.transform),
                adaptedAggrOp,
                wDef
        ), ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    public WindowAggregateBuilder<T> aggregateBuilder() {
        return new WindowAggregateBuilder<>(streamStage, wDef);
    }
}
