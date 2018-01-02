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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.ComputeStageWM;
import com.hazelcast.jet.SourceWithWatermark;
import com.hazelcast.jet.StageWithTimestamp;
import com.hazelcast.jet.Transform;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Javadoc pending.
 */
public class ComputeStageWMImpl<T>
        extends ComputeStageImpl<T>
        implements ComputeStageWM<T> {

    ComputeStageWMImpl(
            @Nonnull SourceWithWatermark<? extends T> wmSource,
            @Nonnull PipelineImpl pipeline
    ) {
        super(wmSource, pipeline);
    }

    ComputeStageWMImpl(
            @Nonnull ComputeStageWM upstream,
            @Nonnull Transform<? extends T> transform,
            @Nonnull PipelineImpl pipeline
    ) {
        super(upstream, transform, pipeline);
    }

    ComputeStageWMImpl(
            @Nonnull List<ComputeStageWM> upstream,
            @Nonnull Transform<? extends T> transform,
            @Nonnull PipelineImpl pipeline
    ) {
        super(upstream, transform, pipeline);
    }

    @Nonnull @Override
    public StageWithTimestamp<T> timestamp(@Nonnull DistributedToLongFunction<? super T> timestampFn) {
        return new StageWithTimestampImpl<>(this, timestampFn);
    }

}
