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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.impl.transform.Transforms;
import com.hazelcast.jet.pipeline.impl.transform.UnaryTransform;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;

import java.util.Map.Entry;

public interface PStream<E> extends PElement {
    <R> PStream<R> apply(UnaryTransform<? super E, R> unaryTransform);

    default <R> PStream<R> map(DistributedFunction<? super E, ? extends R> mapF) {
        return apply(Transforms.map(mapF));
    }

    default <R> PStream<R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapF) {
        return apply(Transforms.flatMap(flatMapF));
    }

    PEnd drainTo(Sink sink);

    default <K, R> PStream<Entry<K, R>> groupBy(DistributedFunction<? super E, ? extends K> keyF,
                                                AggregateOperation1<E, ?, R> aggrOp
    ) {
        return apply(Transforms.groupBy(keyF, aggrOp));
    }

    <K, E1> PStream<Tuple2<E, Iterable<E1>>> join(
            PStream<E1> s1, JoinOn<K, E, E1> joinOn
    );

    <K1, E1, K2, E2> PStream<Tuple3<E, Iterable<E1>, Iterable<E2>>> join(
            PStream<E1> s1, JoinOn<K1, E, E1> joinOn1,
            PStream<E2> s2, JoinOn<K2, E, E2> joinOn2
    );

    default JoinBuilder<E> joinBuilder() {
        return new JoinBuilder<>(this);
    }

    <K, A, E1, R> PStream<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyF,
            PStream<E1> s1, DistributedFunction<? super E1, ? extends K> key1F,
            AggregateOperation2<E, E1, A, R> aggrOp
    );

    <K, A, E1, E2, R> PStream<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyF,
            PStream<E1> s1, DistributedFunction<? super E1, ? extends K> key1F,
            PStream<E2> s2, DistributedFunction<? super E2, ? extends K> key2F,
            AggregateOperation3<E, E1, E2, A, R> aggrOp
    );

    default <K> CoGroupBuilder<K, E> coGroupBuilder(
            DistributedFunction<? super E, K> thisKeyF
    ) {
        return new CoGroupBuilder<>(this, thisKeyF);
    }
}
