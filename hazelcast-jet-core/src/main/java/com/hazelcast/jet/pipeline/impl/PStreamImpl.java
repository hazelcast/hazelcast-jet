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

import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.PElement;
import com.hazelcast.jet.pipeline.PEnd;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.PTransform;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Transform;
import com.hazelcast.jet.pipeline.Tuple2;
import com.hazelcast.jet.pipeline.Tuple3;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
public class PStreamImpl<E> extends AbstractPElement implements PStream<E> {

    public PStreamImpl(List<PElement> upstream, PTransform transform, PipelineImpl pipeline) {
        super(upstream, transform, pipeline);
    }

    public PStreamImpl(PElement upstream, PTransform transform, PipelineImpl pipeline) {
        super(singletonList(upstream), transform, pipeline);
    }

    @Override
    public <R> PStream<R> apply(Transform<? super E, R> transform) {
        return pipeline.apply(this, transform);
    }

    @Override
    public <K, E1> PStream<Tuple2<E, E1>> join(
            PStream<E1> s1,
            JoinClause<K, E, E1> clause
    ) {
        return new PStreamImpl<>(asList(this, s1), new JoinTransform(singletonList(clause)), pipeline);
    }

    @Override
    public <K1, E1, K2, E2> PStream<Tuple3<E, E1, E2>> join(
            PStream<E1> s1,
            JoinClause<K1, E, E1> clause1,
            PStream<E2> s2,
            JoinClause<K2, E, E2> clause2
    ) {
        return new PStreamImpl<>(asList(this, s1, s2), new JoinTransform(asList(clause1, clause2)), pipeline);
    }

    @Override
    public PEnd drainTo(Sink sink) {
        return pipeline.drainTo(this, sink);
    }

    @Override
    public Pipeline getPipeline() {
        return pipeline;
    }
}
