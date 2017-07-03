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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedFunction;

import java.util.Map.Entry;

public interface PStream<E> extends PElement {
    <R> PStream<R> apply(Transform<? super E, R> transform);

    PEnd drainTo(Sink sink);

    <K, E1> PStream<Tuple2<E, E1>> join(
            PStream<E1> s1, JoinClause<K, E, E1> clause
    );

    <K1, E1, K2, E2> PStream<Tuple3<E, E1, E2>> join(
            PStream<E1> s1, JoinClause<K1, E, E1> clause1,
            PStream<E2> s2, JoinClause<K2, E, E2> clause2
    );

    default JoinBuilder<E> joinBuilder() {
        return new JoinBuilder<>(this);
    }

    default <R> PStream<R> map(DistributedFunction<? super E, ? extends R> mapper) {
        return apply(Transforms.map(mapper));
    }

    default <R> PStream<R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapF) {
        return apply(Transforms.flatMap(flatMapF));
    }

    default <K, R> PStream<Entry<K, R>> groupBy(DistributedFunction<? super E, ? extends K> keyF,
                                                AggregateOperation<E, ?, R> aggregation
    ) {
        return apply(Transforms.groupBy(keyF, aggregation));
    }
}
