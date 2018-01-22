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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.transform.CoAggregateTransform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DONT_ADAPT;
import static java.util.stream.Collectors.toList;

public class AggBuilder<T0> {
    @Nonnull
    private final List<GeneralStage> upstreamStages = new ArrayList<>();
    @Nullable
    private final WindowDefinition wDef;
    @Nonnull
    private final PipelineImpl pipelineImpl;

    public AggBuilder(
            @Nonnull GeneralStage<T0> stage0,
            @Nullable WindowDefinition wDef
    ) {
        this.wDef = wDef;
        this.pipelineImpl = ((AbstractStage) stage0).pipelineImpl;
        add(stage0);
    }

    @Nonnull
    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(@Nonnull GeneralStage<E> stage) {
        upstreamStages.add(stage);
        return (Tag<E>) tag(upstreamStages.size() - 1);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <A, R, OUT, OUT_STAGE extends GeneralStage<OUT>> OUT_STAGE build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull CreateOutStageFn<OUT, OUT_STAGE> createOutStageFn
    ) {
        AggregateOperationImpl rawAggrOp = (AggregateOperationImpl) aggrOp;
        DistributedBiConsumer[] adaptedAccumulateFns = new DistributedBiConsumer[rawAggrOp.arity()];
        Arrays.setAll(adaptedAccumulateFns, i ->
                ((ComputeStageImplBase) upstreamStages.get(i)).fnAdapters.adaptAccumulateFn(rawAggrOp.accumulateFn(i))
        );
        AggregateOperation adaptedAggrOp = rawAggrOp.withAccumulateFns(adaptedAccumulateFns);

        List<Transform> upstreamTransforms = upstreamStages.stream().map(AbstractStage::transformOf).collect(toList());
        CoAggregateTransform<A, R> transform = new CoAggregateTransform<>(upstreamTransforms, adaptedAggrOp, wDef);
        OUT_STAGE attached = createOutStageFn.get(transform, DONT_ADAPT, pipelineImpl);
        pipelineImpl.connect(upstreamTransforms, transform);
        return attached;
    }

    @FunctionalInterface
    public interface CreateOutStageFn<OUT, OUT_STAGE extends GeneralStage<OUT>> {
        OUT_STAGE get(Transform transform, FunctionAdapters fnAdapters, PipelineImpl pipeline);
    }
}
