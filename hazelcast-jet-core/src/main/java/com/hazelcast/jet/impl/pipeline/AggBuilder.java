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

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.StageWithTimestamp;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.datamodel.Tag.tag;

public class AggBuilder<T0> {
    @Nonnull
    private final List<ComputeStage> stages = new ArrayList<>();
    @Nullable
    private final WindowDefinition wDef;
    @Nullable
    private final List<DistributedToLongFunction<?>> timestampFns;

    public AggBuilder(
            @Nonnull ComputeStage<T0> s,
            @Nullable DistributedToLongFunction<? super T0> timestampFn,
            @Nullable WindowDefinition wDef
    ) {
        this.wDef = wDef;
        if (timestampFn != null) {
            timestampFns = new ArrayList<>();
            timestampFns.add(timestampFn);
        } else {
            timestampFns = null;
        }
        add(s);
    }

    @Nonnull
    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    @Nonnull
    public <E> Tag<E> add(@Nonnull StageWithTimestamp<E> stage) {
        Objects.requireNonNull(timestampFns, "add(StageWithTimestamp) called on non-windowing AggBuilder");
        StageWithTimestampImpl<E> withTs = (StageWithTimestampImpl<E>) stage;
        timestampFns.add(withTs.timestampFn());
        return add(withTs.computeStage);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(@Nonnull ComputeStage<E> stage) {
        stages.add(stage);
        return (Tag<E>) tag(stages.size() - 1);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <A, R, OUT> ComputeStage<OUT> build(@Nonnull AggregateOperation<A, R> aggrOp) {
        CoAggregateTransform<A, R, OUT> transform = new CoAggregateTransform<>(aggrOp, timestampFns, wDef);
        PipelineImpl pipeline = (PipelineImpl) stages.get(0).getPipeline();
        return pipeline.attach(transform, stages);
    }
}
