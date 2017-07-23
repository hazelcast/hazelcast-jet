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

package com.hazelcast.jet.pipeline.impl;

import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.JoinOn;
import com.hazelcast.jet.pipeline.PElement;
import com.hazelcast.jet.pipeline.PEnd;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.bag.ThreeBags;
import com.hazelcast.jet.pipeline.bag.TwoBags;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.transform.HashJoinTransform;
import com.hazelcast.jet.pipeline.impl.transform.PTransform;
import com.hazelcast.jet.pipeline.impl.transform.UnaryTransform;
import com.hazelcast.jet.pipeline.tuple.Tuple2;
import com.hazelcast.jet.pipeline.tuple.Tuple3;

import java.util.List;

import static com.hazelcast.jet.pipeline.bag.Tag.tag1;
import static com.hazelcast.jet.pipeline.bag.Tag.tag2;
import static com.hazelcast.jet.pipeline.bag.Tag.tag3;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
@SuppressWarnings("unchecked")
public class PStreamImpl<E> extends AbstractPElement implements PStream<E> {

    public PStreamImpl(List<PElement> upstream, PTransform transform, PipelineImpl pipeline) {
        super(upstream, transform, pipeline);
    }

    public PStreamImpl(PElement upstream, PTransform transform, PipelineImpl pipeline) {
        super(singletonList(upstream), transform, pipeline);
    }

    @Override
    public <R> PStream<R> apply(UnaryTransform<? super E, R> unaryTransform) {
        return pipeline.transform(this, unaryTransform);
    }

    @Override
    public <K, E2> PStream<Tuple2<E, Iterable<E2>>> join(
            PStream<E2> s2, JoinOn<K, E, E2> joinOn
    ) {
        return pipeline.attach(asList(this, s2), new HashJoinTransform(singletonList(joinOn), TwoBags.class));
    }

    @Override
    public <K2, E2, K3, E3> PStream<Tuple3<E, Iterable<E2>, Iterable<E3>>> join(
            PStream<E2> s2, JoinOn<K2, E, E2> joinOn1,
            PStream<E3> s3, JoinOn<K3, E, E3> joinOn2
    ) {
        return pipeline.attach(asList(this, s2, s3), new HashJoinTransform(asList(joinOn1, joinOn2), ThreeBags.class));
    }

    @Override
    public <K, A, E2, R> PStream<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyF,
            PStream<E2> s2, DistributedFunction<? super E2, ? extends K> key2F,
            AggregateOperation2<E, E2, A, R> aggrOp
    ) {
        return pipeline.attach(asList(this, s2),
                new CoGroupTransform<>(asList(thisKeyF, key2F), aggrOp, asList(tag1(), tag2())));
    }

    @Override
    public <K, A, E2, E3, R> PStream<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyF,
            PStream<E2> s2, DistributedFunction<? super E2, ? extends K> key2F,
            PStream<E3> s3, DistributedFunction<? super E3, ? extends K> key3F,
            AggregateOperation3<E, E2, E3, A, R> aggrOp
    ) {
        return pipeline.attach(asList(this, s2, s3),
                new CoGroupTransform<>(asList(thisKeyF, key2F, key3F), aggrOp, asList(tag1(), tag2(), tag3())));
    }

    @Override
    public PEnd drainTo(Sink sink) {
        return pipeline.drainTo(this, sink);
    }
}
