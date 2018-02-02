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
import com.hazelcast.jet.impl.pipeline.transform.CoGroupTransform;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithGrouping;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.WindowGroupAggregateBuilder;

import javax.annotation.Nonnull;

import java.util.stream.Stream;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.stream.DistributedCollectors.toList;
import static java.util.Arrays.asList;

public class StageWithGroupingAndWindowImpl<T, K>
        extends StageWithGroupingBase<T, K>
        implements StageWithGroupingAndWindow<T, K> {

    @Nonnull
    private final WindowDefinition wDef;

    StageWithGroupingAndWindowImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull WindowDefinition wDef
    ) {
        super(computeStage, keyFn);
        this.wDef = wDef;
    }

    @Nonnull @Override
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <A, R> StreamStage<TimestampedEntry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        AggregateOperation1<?, ?, ?> adaptedAggrOp = (AggregateOperation1)
                ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp);
        return computeStage.attach(new GroupTransform(
                        computeStage.transform,
                        ADAPT_TO_JET_EVENT.adaptKeyFn(keyFn()),
                        adaptedAggrOp, wDef),
                ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, A, R> StreamStage<TimestampedEntry<K, R>> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        AggregateOperation<?, ?> adaptedAggrOp = ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp);
        return computeStage.attach(new CoGroupTransform(
                asList(computeStage.transform, stageImpl1.transform),
                Stream.of(keyFn(), stage1.keyFn()).map(ADAPT_TO_JET_EVENT::adaptKeyFn).collect(toList()),
                adaptedAggrOp, wDef
        ), ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R> StreamStage<TimestampedEntry<K, R>> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase stageImpl2 = ((StageWithGroupingBase) stage2).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        AggregateOperation<?, ?> adaptedAggrOp = ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp);
        return computeStage.attach(
                new CoGroupTransform(
                        asList(computeStage.transform, stageImpl1.transform, stageImpl2.transform),
                        Stream.of(keyFn(), stage1.keyFn(), stage2.keyFn())
                              .map(ADAPT_TO_JET_EVENT::adaptKeyFn).collect(toList()),
                        adaptedAggrOp, wDef
                ), ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    public WindowGroupAggregateBuilder<T, K> aggregateBuilder() {
        return new WindowGroupAggregateBuilder<>(this);
    }
}
