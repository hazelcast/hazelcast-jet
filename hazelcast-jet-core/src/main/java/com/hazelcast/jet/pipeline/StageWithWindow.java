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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;

/**
 * Javadoc pending.
 */
public interface StageWithWindow<T> {

    WindowDefinition windowDefinition();

    <K> StageWithGroupingAndWindow<T, K> groupingKey(DistributedFunction<? super T, ? extends K> keyFn);

    <A, R> BatchStage<TimestampedEntry<Void, R>> aggregate(
            AggregateOperation1<? super T, A, ? extends R> aggrOp
    );

    <T1, A, R> BatchStage<TimestampedEntry<Void, R>> aggregate2(
            StreamStage<T1> stage1,
            AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp);

    <T1, T2, A, R> BatchStage<TimestampedEntry<Void, R>> aggregate3(
            StreamStage<T1> stage1,
            StreamStage<T2> stage2,
            AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp);

    WindowAggregateBuilder<T> aggregateBuilder();
}
