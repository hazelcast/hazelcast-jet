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
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.pipeline.transform.WindowCoGroupTransform;
import com.hazelcast.jet.impl.pipeline.transform.WindowGroupTransform;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithGrouping;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.WindowGroupAggregateBuilder;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
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

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<K, R, OUT> mapToOutputFn
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        AggregateOperation1<JetEvent<T>, A, R> adaptedAggrOp = (AggregateOperation1)
                fnAdapter.adaptAggregateOperation(aggrOp);
        return computeStage.attach(new WindowGroupTransform<JetEvent<T>, K, A, R, JetEvent<OUT>>(
                        computeStage.transform,
                        wDef,
                        fnAdapter.adaptKeyFn(keyFn()),
                        adaptedAggrOp,
                        fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <A, R> StreamStage<TimestampedEntry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return aggregate(aggrOp, TimestampedEntry::new);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<K, R, OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        AggregateOperation<A, R> adaptedAggrOp = fnAdapter.adaptAggregateOperation(aggrOp);
        return computeStage.attach(new WindowCoGroupTransform<>(
                asList(computeStage.transform, stageImpl1.transform),
                wDef,
                asList(fnAdapter.adaptKeyFn(keyFn()),
                        fnAdapter.adaptKeyFn(stage1.keyFn())),
                adaptedAggrOp,
                mapToOutputFn
        ), fnAdapter);
    }

    @Nonnull @Override
    public <T1, A, R> StreamStage<TimestampedEntry<K, R>> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp
    ) {
        return aggregate2(stage1, aggrOp, TimestampedEntry::new);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<K, R, OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase stageImpl2 = ((StageWithGroupingBase) stage2).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        AggregateOperation<A, R> adaptedAggrOp = fnAdapter.adaptAggregateOperation(aggrOp);
        return computeStage.attach(
                new WindowCoGroupTransform<>(
                        asList(computeStage.transform, stageImpl1.transform, stageImpl2.transform),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()),
                                fnAdapter.adaptKeyFn(stage1.keyFn()),
                                fnAdapter.adaptKeyFn(stage2.keyFn())),
                        adaptedAggrOp,
                        mapToOutputFn
                ), fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R> StreamStage<TimestampedEntry<K, R>> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp
    ) {
        return aggregate3(stage1, stage2, aggrOp, TimestampedEntry::new);
    }

    @Nonnull @Override
    public WindowGroupAggregateBuilder<T, K> aggregateBuilder() {
        return new WindowGroupAggregateBuilder<>(this);
    }
}
