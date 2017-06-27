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

import com.hazelcast.jet.pipeline.PElement;
import com.hazelcast.jet.pipeline.PStream;
import com.hazelcast.jet.pipeline.PEnd;
import com.hazelcast.jet.pipeline.PTransform;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Transform;

/**
 * Javadoc pending.
 */
public class PStreamImpl<E> extends AbstractPElement implements PStream<E> {

    PStreamImpl(PElement upstream, PTransform transform, PipelineImpl pipeline) {
        super(upstream, transform, pipeline);
    }

    @Override
    public <R> PStream<R> apply(Transform<E, R> transform) {
        return pipeline.apply(this, transform);
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
