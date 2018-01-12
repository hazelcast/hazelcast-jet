/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.impl.pipeline.AggBuilder.CreateOutStageFn;
import com.hazelcast.jet.impl.pipeline.ComputeStageWMImpl;
import com.hazelcast.jet.impl.pipeline.GrAggBuilder;

public class WindowGroupAggregateBuilder<T0, K> {
    private final GrAggBuilder<K> graggBuilder;

    public WindowGroupAggregateBuilder(StageWithGroupingAndWindow<T0, K> s) {
        graggBuilder = new GrAggBuilder<>(s);
    }

    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(StageWithGroupingWM<E, K> stage) {
        return graggBuilder.add(stage);
    }

    public <A, R> ComputeStageWM<TimestampedEntry<K, R>> build(AggregateOperation<A, R> aggrOp) {
        CreateOutStageFn<TimestampedEntry<K, R>, ComputeStageWM<TimestampedEntry<K, R>>> createOutStageFn
                = ComputeStageWMImpl<TimestampedEntry<K, R>>::new;
        return graggBuilder.build(aggrOp, createOutStageFn);
    }
}
