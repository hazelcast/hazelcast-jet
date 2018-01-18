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

import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;

public class AggBuilder<T0> {
    @Nonnull
    private final List<Transform> upstream = new ArrayList<>();
    @Nullable
    private final WindowDefinition wDef;
    private final PipelineImpl pipelineImpl;

    public AggBuilder(
            @Nonnull GeneralStage<T0> s,
            @Nullable WindowDefinition wDef
    ) {
        this.wDef = wDef;
        this.pipelineImpl = ((AbstractStage) s).pipelineImpl;
        add(s);
    }

    @Nonnull
    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(@Nonnull GeneralStage<E> stage) {
        upstream.add(transformOf(stage));
        return (Tag<E>) tag(upstream.size() - 1);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <A, R, OUT, OUT_STAGE extends GeneralStage<OUT>> OUT_STAGE build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull CreateOutStageFn<OUT, OUT_STAGE> createOutStageFn
    ) {
        CoAggregateTransform<A, R, OUT> transform = new CoAggregateTransform<>(upstream, aggrOp, wDef);
        OUT_STAGE attached = createOutStageFn.get(transform, pipelineImpl);
        pipelineImpl.connect(upstream, transform);
        return attached;
    }

    @FunctionalInterface
    public interface CreateOutStageFn<OUT, OUT_STAGE extends GeneralStage<OUT>> {
        OUT_STAGE get(Transform transform, PipelineImpl pipeline);
    }
}
