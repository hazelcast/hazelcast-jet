/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.impl.metrics.UserMetricsUtil;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.pipeline.transform.WindowGroupTransform;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StageWithKeyAndWindowImpl<T, K>
        extends StageWithGroupingBase<T, K>
        implements StageWithKeyAndWindow<T, K> {

    @Nonnull
    private final WindowDefinition wDef;

    StageWithKeyAndWindowImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
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
    public <R> StreamStage<KeyedWindowResult<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        List<Serializable> metricsProviderCandidates = asList(
                aggrOp.accumulateFn(), aggrOp.createFn(), aggrOp.combineFn(),
                aggrOp.deductFn(), aggrOp.exportFn(), aggrOp.finishFn());
        return attachAggregate(UserMetricsUtil.wrapAll(aggrOp, metricsProviderCandidates));
    }

    private <R> StreamStage<KeyedWindowResult<K, R>> attachAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp) {
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R>(
                        singletonList(computeStage.transform),
                        wDef,
                        singletonList(fnAdapter.adaptKeyFn(keyFn())),
                        fnAdapter.adaptAggregateOperation1(aggrOp)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    public <T1, R> StreamStage<KeyedWindowResult<K, R>> aggregate2(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(((StageWithGroupingBase) stage1).computeStage, "stage1");
        Transform upstream1 = ((StageWithGroupingBase) stage1).computeStage.transform;
        List<Serializable> metricsProviderCandidates = asList(
                aggrOp.accumulateFn0(), aggrOp.accumulateFn1(), aggrOp.createFn(), aggrOp.combineFn(),
                aggrOp.deductFn(), aggrOp.exportFn(), aggrOp.finishFn());
        return attachAggregate2(stage1, upstream1, UserMetricsUtil.wrapAll(aggrOp, metricsProviderCandidates));
    }

    private <T1, R> StreamStage<KeyedWindowResult<K, R>> attachAggregate2(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1, Transform upstream1, @
            Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp) {
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R>(
                        asList(computeStage.transform, upstream1),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()), fnAdapter.adaptKeyFn(stage1.keyFn())),
                        fnAdapter.adaptAggregateOperation2(aggrOp)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    public <T1, T2, R> StreamStage<KeyedWindowResult<K, R>> aggregate3(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase stageImpl2 = ((StageWithGroupingBase) stage2).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        Transform transform1 = ((StageWithGroupingBase) stage1).computeStage.transform;
        Transform transform2 = ((StageWithGroupingBase) stage2).computeStage.transform;

        List<Serializable> metricsProviderCandidates = asList(
                aggrOp.accumulateFn0(), aggrOp.accumulateFn1(), aggrOp.accumulateFn2(), aggrOp.createFn(),
                aggrOp.combineFn(), aggrOp.deductFn(), aggrOp.exportFn(), aggrOp.finishFn());
        return attachAggregate3(stage1, stage2, transform1, transform2,
                UserMetricsUtil.wrapAll(aggrOp, metricsProviderCandidates));
    }

    private <T1, T2, R> StreamStage<KeyedWindowResult<K, R>> attachAggregate3(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            Transform transform1, Transform transform2,
            AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R>(
                        asList(computeStage.transform, transform1, transform2),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()),
                                fnAdapter.adaptKeyFn(stage1.keyFn()),
                                fnAdapter.adaptKeyFn(stage2.keyFn())),
                        fnAdapter.adaptAggregateOperation3(aggrOp)
                ),
                fnAdapter);
    }

}
