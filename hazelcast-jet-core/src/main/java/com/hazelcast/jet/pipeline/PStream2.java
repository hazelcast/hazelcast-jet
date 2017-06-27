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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.function.DistributedFunction;

import java.util.Map.Entry;

public interface PStream2<E1, E2> {

    <R> void apply(Transform<PStream2<E1, E2>, PStream<R>> transform);

    default <K> PStream<Tuple2<E1, E2>> hashJoin(DistributedFunction<E1, K> keyF, DistributedFunction<E2, K> keyF2) {

    }

    default <K,R> PStream<Entry<K, R>> coGroup(DistributedFunction<E1, K> keyF, DistributedFunction<E2, K> keyF2,
                                               AggregateOperation<Tuple2<E1, E2>, ?, R> aggregateOp) {

    }


}
