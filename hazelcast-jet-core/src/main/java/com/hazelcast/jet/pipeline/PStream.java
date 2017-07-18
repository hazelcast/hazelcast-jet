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
import com.hazelcast.jet.pipeline.bag.TwoBags;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.impl.transform.Transforms;
import com.hazelcast.jet.pipeline.impl.transform.UnaryTransform;
import com.hazelcast.jet.pipeline.tuple.Tuple2;

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
                                                AggregateOperation<E, ?, R> aggrOp
    ) {
        return apply(Transforms.groupBy(keyF, aggrOp));
    }

    <K, E2> PStream<TwoBags<E, E2>> join(
            PStream<E2> s1, JoinOn<K, E, E2> joinOn
    );

    <K2, E2, K3, E3> PStream<ThreeBags<E, E2, E3>> join(
            PStream<E2> s1, JoinOn<K2, E, E2> joinOn1,
            PStream<E3> s2, JoinOn<K3, E, E3> joinOn2
    );

    default JoinBuilder<E> joinBuilder() {
        return new JoinBuilder<>(this);
    }

    <K, A, E2, R> PStream<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyF,
            PStream<E2> s2, DistributedFunction<? super E2, ? extends K> key2F,
            GroupAggregation<TwoBags<E, E2>, A, R> groupAggr
    );

    <K, A, E2, E3, R> PStream<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyF,
            PStream<E2> s2, DistributedFunction<? super E2, ? extends K> key2F,
            PStream<E3> s3, DistributedFunction<? super E3, ? extends K> key3F,
            GroupAggregation<ThreeBags<E, E2, E3>, A, R> groupAggr
    );

    default <K> CoGroupBuilder<K, E> coGroupBuilder(
            DistributedFunction<? super E, K> thisKeyF
    ) {
        return new CoGroupBuilder<>(this, thisKeyF);
    }
}
