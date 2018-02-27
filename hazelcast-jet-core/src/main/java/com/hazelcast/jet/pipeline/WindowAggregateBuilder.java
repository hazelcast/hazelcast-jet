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

/**
 * Offers a step-by-step fluent API to build a pipeline stage that
 * performs a windowed co-aggregation of the data from several input
 * stages. To obtain it, call {@link StageWithWindow#aggregateBuilder()} on
 * one of the stages to co-aggregate, then add the other stages by calling
 * {@link #add add(stage)} on the builder. Collect all the tags returned
 * from {@code add()} and use them when building the aggregate operation.
 * Retrieve the tag of the first stage (from which you obtained the
 * builder) by calling {@link #tag0()}.
 * <p>
 * This object is mainly intended to build a co-aggregation of four or more
 * contributing stages. For up to three stages, prefer the direct {@code
 * stage.aggregateN(...)} calls because they offer more static type safety.
 *
 * @param <T0> the type of the stream-0 item
 */
public class WindowAggregateBuilder<T0> {
    @Nonnull
    private final AggBuilder<T0> aggBuilder;

    WindowAggregateBuilder(@Nonnull StreamStage<T0> s, @Nonnull WindowDefinition wDef) {
        this.aggBuilder = new AggBuilder<>(s, wDef);
    }

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    public <E> Tag<E> add(StreamStage<E> stage) {
        return aggBuilder.add(stage);
    }

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * co-aggregation of the pipeline stages registered with this builder
     * object. The tags you register with the aggregate operation must match
     * the tags you registered with this builder. For example,
     * <pre>{@code
     * StageWithWindow<A> stage0 = streamStage0.window(...);
     * StreamStage<B> stage1 = p.drawFrom(Sources.mapJournal("b", ...));
     * StreamStage<C> stage2 = p.drawFrom(Sources.mapJournal("c", ...));
     * StreamStage<D> stage3 = p.drawFrom(Sources.mapJournal("d", ...));
     *
     * WindowAggregateBuilder<A> builder = stage0.aggregateBuilder();
     * Tag<A> tagA = builder.tag0();
     * Tag<B> tagB = builder.add(stage1);
     * Tag<C> tagC = builder.add(stage2);
     * Tag<D> tagD = builder.add(stage3);
     * StreamStage<TimestampedItem<Result>> = builder.build(AggregateOperation
     *         .withCreate(MyAccumulator::new)
     *         .andAccumulate(tagA, MyAccumulator::put)
     *         .andAccumulate(tagB, MyAccumulator::put)
     *         .andAccumulate(tagC, MyAccumulator::put)
     *         .andAccumulate(tagD, MyAccumulator::put)
     *         .andCombine(MyAccumulator::combine)
     *         .andFinish(MyAccumulator::finish));
     * }</pre>
     *
     * @param aggrOp        the aggregate operation to perform
     * @param mapToOutputFn a function that creates the output item from the aggregation result
     * @param <A>           the type of items in the pipeline stage this builder was obtained from
     * @param <R>           the type of the aggregation result
     * @param <OUT>         the type of the output item
     * @return a new stage representing the co-aggregation
     */
    public <A, R, OUT> StreamStage<OUT> build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        CreateOutStageFn<OUT, StreamStage<OUT>> createOutStageFn = StreamStageImpl::new;
        return aggBuilder.build(aggrOp, createOutStageFn, mapToOutputFn);
    }

    /**
     * Convenience for {@link #build(AggregateOperation, WindowResultFunction)
     * build(aggrOp, mapToOutputFn)} which emits {@code TimestampedItem}s as output.
     * The timestamp corresponds to the window's end.
     *
     * @param aggrOp the aggregate operation to perform.
     * @param <A>    the type of items in the pipeline stage this builder was obtained from
     * @param <R>    the type of the aggregation result
     * @return a new stage representing the co-group-and-aggregate operation
     */
    public <A, R> StreamStage<TimestampedItem<R>> build(@Nonnull AggregateOperation<A, R> aggrOp) {
        return build(aggrOp, TimestampedItem::new);
    }
}
