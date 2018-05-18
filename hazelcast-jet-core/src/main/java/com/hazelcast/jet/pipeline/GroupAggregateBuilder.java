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

import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.pipeline.GrAggBuilder;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;

/**
 * Offers a step-by-step fluent API to build a pipeline stage that
 * co-groups and aggregates the data from several input stages. To obtain
 * it, call {@link StageWithGrouping#aggregateBuilder()} on one of the
 * stages to co-group, then add the other stages by calling {@link #add
 * add(stage)} on the builder. Collect all the tags returned from {@code
 * add()} and use them when building the aggregate operation. Retrieve the
 * tag of the first stage (from which you obtained the builder) by calling
 * {@link #tag0()}.
 * <p>
 * This object is mainly intended to build a co-grouping of four or more
 * contributing stages. For up to three stages, prefer the direct {@code
 * stage.aggregateN(...)} calls because they offer more static type safety.
 *
 * @param <K> type of the grouping key
 * @param <R0> type of the aggregation result for stream-0
 */
public class GroupAggregateBuilder<K, R0> {
    private final GrAggBuilder<K> grAggBuilder;
    private final CoAggregateOperationBuilder aggropBuilder = coAggregateOperationBuilder();

    <T0> GroupAggregateBuilder(
            StageWithGrouping<T0, K> stage0,
            AggregateOperation1<? super T0, ?, ? extends R0> aggrOp0
    ) {
        grAggBuilder = new GrAggBuilder<>(stage0);
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
            StageWithGrouping<T, K> stage,
            AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        Tag<T> tag = grAggBuilder.add(stage);
        return aggropBuilder.add(tag, aggrOp);
    }

    /**
     * Creates and returns a pipeline stage that performs the
     * co-grouping and aggregation of pipeline stages registered with this
     * builder object. The tags you register with the aggregate operation must
     * match the tags you registered with this builder. For example,
     * <pre>{@code
     * StageWithGrouping<A, String> stage0 = batchStage0.groupingKey(A::key);
     * StageWithGrouping<B, String> stage1 = batchStage1.groupingKey(B::key);
     * StageWithGrouping<C, String> stage2 = batchStage2.groupingKey(C::key);
     * StageWithGrouping<D, String> stage3 = batchStage3.groupingKey(D::key);
     *
     * AggregateBuilder<A> builder = stage0.aggregateBuilder();
     * Tag<A> tagA = builder.tag0();
     * Tag<B> tagB = builder.add(stage1, B::key);
     * Tag<C> tagC = builder.add(stage2, C::key);
     * Tag<D> tagD = builder.add(stage3, D::key);
     * BatchStage<Result> resultStage = builder.build(AggregateOperation
     *         .withCreate(MyAccumulator::new)
     *         .andAccumulate(tagA, MyAccumulator::put)
     *         .andAccumulate(tagB, MyAccumulator::put)
     *         .andAccumulate(tagC, MyAccumulator::put)
     *         .andAccumulate(tagD, MyAccumulator::put)
     *         .andCombine(MyAccumulator::combine)
     *         .andFinish(MyAccumulator::finish),
     *     (String key, String result) -> Util.entry(key, result)
     * );
     * }</pre>
     *
     * @param <OUT> the output item type
     * @return a new stage representing the co-aggregation
     */
    public <OUT> BatchStage<OUT> build(
            @Nonnull DistributedBiFunction<? super K, ItemsByTag, OUT> mapToOutputFn
    ) {
        AggregateOperation<Object[], ItemsByTag> aggrOp = aggropBuilder.build();
        return grAggBuilder.buildBatch(aggrOp, mapToOutputFn);
    }

    /**
     * Convenience for {@link #build(DistributedBiFunction)
     * build(mapToOutputFn)} which emits {@code Map.Entry}s as output.
     *
     * @return a new stage representing the co-group-and-aggregate operation
     */
    public BatchStage<Entry<K, ItemsByTag>> build() {
        return build(Util::entry);
    }
}
