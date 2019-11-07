/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

public class StreamSourceStageImpl<T> implements StreamSourceStage<T> {

    private final StreamSourceTransform<T> transform;
    private final PipelineImpl pipeline;

    StreamSourceStageImpl(StreamSourceTransform<T> transform, PipelineImpl pipeline) {
        this.transform = transform;
        this.pipeline = pipeline;
    }

    @Override
    public StreamStage<T> withNativeTimestamps(long allowedLag) {
        checkTrue(transform.supportsNativeTimestamps(), "The source doesn't support native timestamps");
        transform.setEventTimePolicy(eventTimePolicy(
                null,
                JetEvent::jetEvent,
                limitingLag(allowedLag),
                0,
                0,
                transform.partitionIdleTimeout()
        ));
        return new StreamStageImpl<>(transform, pipeline);
    }

    @Override
    public StreamStage<T> withTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLag) {
        checkSerializable(timestampFn, "timestampFn");
        transform.setEventTimePolicy(eventTimePolicy(
                timestampFn,
                JetEvent::jetEvent,
                limitingLag(allowedLag),
                0,
                0,
                transform.partitionIdleTimeout()
        ));
        return new StreamStageImpl<>(transform, pipeline);
    }

    @Override
    public StreamStage<T> withoutTimestamps() {
        transform.setEventTimePolicy(eventTimePolicy(
                t -> JetEvent.NO_TIMESTAMP,
                JetEvent::jetEvent,
                limitingLag(0),
                0,
                0,
                transform.partitionIdleTimeout()
        ));
        return new StreamStageImpl<>(transform, pipeline);
    }
}
