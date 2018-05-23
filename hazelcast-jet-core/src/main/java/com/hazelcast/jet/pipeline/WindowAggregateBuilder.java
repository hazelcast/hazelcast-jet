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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.pipeline.AggBuilder;
import com.hazelcast.jet.impl.pipeline.AggBuilder.CreateOutStageFn;
import com.hazelcast.jet.impl.pipeline.StreamStageImpl;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;

/**
 * Offers a step-by-step fluent API to build a pipeline stage that
 * performs a windowed co-aggregation of the data from several input
 * stages. To obtain it, call {@link StageWithWindow#aggregateBuilder()} on
 * one of the stages to co-aggregate and refer to that method's Javadoc for
 * further details.
 *
 * @param <R0> type of the aggregated result for stream-0
 */
public class WindowAggregateBuilder<R0> {
    private final AggBuilder aggBuilder;
    private final CoAggregateOperationBuilder aggropBuilder = coAggregateOperationBuilder();

    <T0> WindowAggregateBuilder(
            @Nonnull StreamStage<T0> s,
            @Nonnull AggregateOperation1<? super T0, ?, ? extends R0> aggrOp0,
            @Nonnull WindowDefinition wDef
    ) {
        aggBuilder = new AggBuilder(s, wDef);
        aggropBuilder.add(Tag.tag0(), aggrOp0);
    }

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    public Tag<R0> tag0() {
        return Tag.tag0();
    }

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    public <T, R> Tag<R> add(
            StreamStage<T> stage,
            AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        Tag<T> tag = aggBuilder.add(stage);
        return aggropBuilder.add(tag, aggrOp);
    }

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * co-aggregation of the stages registered with this builder object. The
     * composite aggregate operation places the results of the individual
     * aggregate operations in an {@code ItemsByTag} and the {@code
     * mapToOutputFn} you supply transforms it to the final result to emit. Use
     * the tags you got from this builder in the implementation of {@code
     * mapToOutputFn} to access the results.
     *
     * @param mapToOutputFn the function that transforms the results into the output items
     * @param <OUT> the output item type
     *
     * @return a new stage representing the co-aggregation
     */
    public <OUT> StreamStage<OUT> build(
            @Nonnull WindowResultFunction<? super ItemsByTag, ? extends OUT> mapToOutputFn
    ) {
        AggregateOperation<Object[], ItemsByTag> aggrOp = aggropBuilder.build();
        CreateOutStageFn<OUT, StreamStage<OUT>> createOutStageFn = StreamStageImpl::new;
        return aggBuilder.build(aggrOp, createOutStageFn, mapToOutputFn);
    }

    /**
     * Convenience for {@link #build(WindowResultFunction) build(mapToOutputFn)}
     * which emits {@code TimestampedItem}s as output. The timestamp corresponds
     * to the window's end.
     *
     * @return a new stage representing the cogroup-and-aggregate operation
     */
    public StreamStage<TimestampedItem<ItemsByTag>> build() {
        return build(TimestampedItem::new);
    }
}
