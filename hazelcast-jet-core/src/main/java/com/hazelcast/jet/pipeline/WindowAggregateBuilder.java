/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.pipeline.AggBuilder;
import com.hazelcast.jet.impl.pipeline.AggBuilder.CreateOutStageFn;
import com.hazelcast.jet.impl.pipeline.StreamStageImpl;

import javax.annotation.Nonnull;

public class WindowAggregateBuilder<T0> {
    @Nonnull
    private final AggBuilder<T0> aggBuilder;

    public WindowAggregateBuilder(@Nonnull StreamStage<T0> s, @Nonnull WindowDefinition wDef) {
        this.aggBuilder = new AggBuilder<>(s, wDef);
    }

    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    public <E> Tag<E> add(StreamStage<E> stage) {
        return aggBuilder.add(stage);
    }

    public <A, R, OUT> StreamStage<OUT> build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputfn
    ) {
        CreateOutStageFn<OUT, StreamStage<OUT>> createOutStageFn = StreamStageImpl::new;
        return aggBuilder.build(aggrOp, createOutStageFn, mapToOutputfn);
    }

    public <A, R> StreamStage<TimestampedItem<R>> build(@Nonnull AggregateOperation<A, R> aggrOp) {
        return build(aggrOp, TimestampedItem::new);
    }
}
