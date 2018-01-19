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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowAggregateBuilder;
import com.hazelcast.jet.pipeline.WindowDefinition;

import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static java.util.Arrays.asList;

/**
 * Javadoc pending.
 */
public class StageWithWindowImpl<T> implements StageWithWindow<T> {

    private final StreamStageImpl<T> computeStage;
    private final WindowDefinition wDef;

    StageWithWindowImpl(
            StreamStageImpl<T> computeStage,
            WindowDefinition wDef
    ) {
        this.computeStage = computeStage;
        this.wDef = wDef;
    }

    @Override
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Override
    public <K> StageWithGroupingAndWindow<T, K> groupingKey(DistributedFunction<? super T, ? extends K> keyFn) {
        return new StageWithGroupingAndWindowImpl<>(computeStage, keyFn, wDef);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A, R> BatchStage<TimestampedEntry<Void, R>> aggregate(
            AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return computeStage.attach(new AggregateTransform<T, A, R, TimestampedEntry<Void, R>>(
                computeStage.transform, aggrOp, wDef));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T1, A, R> BatchStage<TimestampedEntry<Void, R>> aggregate2(
            StreamStage<T1> stage1,
            AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return computeStage.attach(new CoAggregateTransform<>(
                asList(computeStage.transform, transformOf(stage1)),
                aggrOp));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R> BatchStage<TimestampedEntry<Void, R>> aggregate3(
            StreamStage<T1> stage1,
            StreamStage<T2> stage2,
            AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return computeStage.attach(new CoAggregateTransform<>(
                asList(computeStage.transform, transformOf(stage1), transformOf(stage2)),
                aggrOp));
    }

    @Override
    public WindowAggregateBuilder<T> aggregateBuilder() {
        return new WindowAggregateBuilder<>(computeStage, wDef);
    }
}
