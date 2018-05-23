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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.WindowResult2Function;
import com.hazelcast.jet.function.WindowResult3Function;
import com.hazelcast.jet.function.WindowResultFunction;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

/**
 * Represents an intermediate step in the construction of a pipeline stage
 * that performs a windowed aggregate operation. You can perform a global
 * aggregation or add a grouping key to perform a group-and-aggregate
 * operation.
 *
 * @param <T> type of the input item
 */
public interface StageWithWindow<T> {

    /**
     * Returns the pipeline stage associated with this object. It is the stage
     * to which you are about to attach an aggregating stage.
     */
    @Nonnull
    StreamStage<T> streamStage();

    /**
     * Returns the definition of the window for the windowed aggregation
     * operation that you are about to construct using this object.
     */
    @Nonnull
    WindowDefinition windowDefinition();

    /**
     * Specifes the function that will extract the grouping key from the
     * items in the associated pipeline stage and moves on to the step in
     * which you'll complete the construction of a windowed group-and-aggregate
     * stage.
     *
     * @param keyFn function that extracts the grouping key
     * @param <K> type of the key
     */
    @Nonnull
    <K> StageWithGroupingAndWindow<T, K> groupingKey(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    );

    /**
     * Javadoc pending.
     *
     * @param keyFn
     * @param mapToOutputFn
     * @param <K>
     * @param <R>
     */
    @Nonnull
    <K, R> StreamStage<R> distinctBy(
            DistributedFunction<? super T, ? extends K> keyFn,
            WindowResultFunction<? super T, ? extends R> mapToOutputFn
    );

    /**
     * Javadoc pending.
     *
     * @param keyFn
     * @param <K>
     */
    @Nonnull
    default <K> StreamStage<TimestampedItem<T>> distinctBy(DistributedFunction<? super T, ? extends K> keyFn) {
        return distinctBy(keyFn, TimestampedItem::new);
    }

    /**
     * Javadoc pending.
     */
    @Nonnull
    default StreamStage<TimestampedItem<T>> distinct() {
        return distinctBy(wholeItem(), TimestampedItem::new);
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate operation
     * over all the items that belong to a given window. Once the window is
     * complete, it invokes {@code mapToOutputFn} with the result of the aggregate
     * operation and emits its return value as the window result.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <A> the type of the accumulator used by the aggregate operation
     * @param <R> the type of the result
     */
    @Nonnull
    <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    );

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to a given window. Once the
     * window is complete, it emits a {@code TimestampedItem} with the result
     * of the aggregate operation and the timestamp denoting the window's
     * ending time.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <A> the type of the accumulator used by the aggregate operation
     * @param <R> the type of the result
     */
    @Nonnull
    default <A, R> StreamStage<TimestampedItem<R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return aggregate(aggrOp, TimestampedItem::new);
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to the same window. It receives
     * the items from both this stage and {@code stage1}. Once a given window
     * is complete, it invokes {@code mapToOutputFn} with the result of the
     * aggregate operation and emits its return value as the window result.
     * <p>
     * This variant requires you to provide a two-input aggregate operation
     * (refer to its {@linkplain AggregateOperation2 Javadoc} for a simple
     * example). If you can express your logic in terms of two single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate2(AggregateOperation1, StreamStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it offers a simpler
     * API and you can use the already defined single-input operations. Use
     * this variant only when you have the need to implement an aggregate
     * operation that combines the input streams into the same accumulator.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     * @param <OUT> type of the output item
     */
    @Nonnull
    <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn);

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to the same window. It receives
     * the items from both this stage and {@code stage1}. Once a given window
     * is complete, it emits a {@link TimestampedItem} with the result
     * of the aggregate operation and the timestamp denoting the window's
     * ending time.
     * <p>
     * This variant requires you to provide a two-input aggregate operation
     * (refer to its {@linkplain AggregateOperation2 Javadoc} for a simple
     * example). If you can express your logic in terms of two single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate2(AggregateOperation1, StreamStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it offers a simpler
     * API and you can use the already defined single-input operations. Use
     * this variant only when you have the need to implement an aggregate
     * operation that combines the input streams into the same accumulator.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    default <T1, A, R> StreamStage<TimestampedItem<R>> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp
    ) {
        return aggregate2(stage1, aggrOp, TimestampedItem::new);
    }

    /**
     * Attaches to this stage a stage that performs the given co-aggregate
     * operation over the items from both this stage and {@code stage1} you
     * supply. It performs the aggregation separately for each input stage:
     * {@code aggrOp0} on this stage and {@code aggrOp1} on {@code stage1}.
     * Once it has received all the items belonging to a window, it calls
     * the supplied {@code mapToOutputFn} with the aggregation results to
     * create the item to emit.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the other stage
     * @param aggrOp1 aggregate operation to perform on the other stage
     * @param mapToOutputFn function to apply to the aggregated results
     * @param <T1> type of the items in the other stage
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for the other stage
     * @param <OUT> the output item type
     */
    @Nonnull
    default <T1, R0, R1, OUT> StreamStage<OUT> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull WindowResult2Function<? super R0, ? super R1, ? extends OUT> mapToOutputFn
    ) {
        return aggregate2(stage1, aggregateOperation2(aggrOp0, aggrOp1, Tuple2::tuple2),
                (start, end, r) -> mapToOutputFn.apply(start, end, r.f0(), r.f1()));
    }

    /**
     * Attaches to this stage a stage that performs the given co-aggregate
     * operation over the items from both this stage and {@code stage1} you
     * supply. It performs the aggregation separately for each input stage:
     * {@code aggrOp0} on this stage and {@code aggrOp1} on {@code stage1}.
     * Once it has received all the items belonging to a window, it emits a
     * {@code TimestampedItem(Tuple2(result0, result1))}.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the other stage
     * @param aggrOp1 aggregate operation to perform on the other stage
     * @param <T1> type of the items in the other stage
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for the other stage
     */
    @Nonnull
    default <T1, R0, R1> StreamStage<TimestampedItem<Tuple2<R0, R1>>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1
    ) {
        AggregateOperation2<T, T1, ?, Tuple2<R0, R1>> aggrOp = aggregateOperation2(aggrOp0, aggrOp1, Tuple2::tuple2);
        return aggregate2(stage1, aggrOp, (start, end, t2) -> new TimestampedItem<>(end, t2));
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over the items it receives from this stage as well as
     * {@code stage1} and {@code stage2} you supply. Once a given window
     * is complete, it invokes {@code mapToOutputFn} with the result of the
     * aggregate operation and emits its return value as the window result.
     * <p>
     * This variant requires you to provide a three-input aggregate operation
     * (refer to its {@linkplain AggregateOperation3 Javadoc} for a simple
     * example). If you can express your logic in terms of three single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate3(AggregateOperation1, StreamStage,
     *      AggregateOperation1, StreamStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1, stage2, aggrOp2)} because it
     * offers a simpler API and you can use the already defined single-input
     * operations. Use this variant only when you have the need to implement an
     * aggregate operation that combines the input streams into the same
     * accumulator.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     * @param <OUT> type of the output item
     */
    @Nonnull
    <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn);

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over the items it receives from this stage as well as
     * {@code stage1} and {@code stage2} you supply. Once a given window
     * is complete, it emits a {@link TimestampedItem} with the result
     * of the aggregate operation and the timestamp denoting the window's
     * ending time.
     * <p>
     * This variant requires you to provide a three-input aggregate operation
     * (refer to its {@linkplain AggregateOperation3 Javadoc} for a simple
     * example). If you can express your logic in terms of three single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate3(AggregateOperation1, StreamStage,
     *      AggregateOperation1, StreamStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1, stage2, aggrOp2)} because it
     * offers a simpler API and you can use the already defined single-input
     * operations. Use this variant only when you have the need to implement an
     * aggregate operation that combines the input streams into the same
     * accumulator.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    default <T1, T2, A, R> StreamStage<TimestampedItem<R>> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp
    ) {
        return aggregate3(stage1, stage2, aggrOp, TimestampedItem::new);
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to the same window. It receives
     * the items from both this stage and {@code stage1}. It performs the
     * aggregation separately for each input stage: {@code aggrOp0} on this
     * stage, {@code aggrOp1} on {@code stage1} and {@code aggrOp2} on {@code
     * stage2}. Once a given window is complete, it invokes {@code
     * mapToOutputFn} with the result of the aggregate operation and emits its
     * return value as the window result.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     *
     * @param mapToOutputFn the function that creates the output item
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <R0> type of the result from stream-0
     * @param <R1> type of the result from stream-1
     * @param <R2> type of the result from stream-2
     * @param <OUT> type of the output item
     */
    @Nonnull
    default <T1, T2, R0, R1, R2, OUT> StreamStage<OUT> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2,
            @Nonnull WindowResult3Function<? super R0, ? super R1, ? super R2, ? extends OUT> mapToOutputFn
    ) {
        return aggregate3(stage1, stage2, aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, Tuple3::tuple3),
                (start, end, t3) -> mapToOutputFn.apply(start, end, t3.f0(), t3.f1(), t3.f2()));
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to the same window. It receives
     * the items from both this stage and {@code stage1}. It performs the
     * aggregation separately for each input stage: {@code aggrOp0} on this
     * stage, {@code aggrOp1} on {@code stage1} and {@code aggrOp2} on {@code
     * stage2}. Once it has received all the items belonging to a window, it
     * emits a {@code TimestampedItem(Tuple3(result0, result1, result2))}.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     *
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <R0> type of the result from stream-0
     * @param <R1> type of the result from stream-1
     * @param <R2> type of the result from stream-2
     */
    @Nonnull
    default <T1, T2, R0, R1, R2> StreamStage<TimestampedItem<Tuple3<R0, R1, R2>>> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2
    ) {
        AggregateOperation3<T, T1, T2, ?, Tuple3<R0, R1, R2>> aggrOp =
                aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, Tuple3::tuple3);
        return aggregate3(stage1, stage2, aggrOp, (start, end, t3) -> new TimestampedItem<>(end, t3));
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get. You supply an aggregate operation
     * for each input stage and in the output you get the individual
     * aggregation results in a {@code TimestampedItem(windowEnd, itemsByTag)}.
     * Use the tag you get from {@link AggregateBuilder#add builder.add(stageN,
     * aggrOpN)} to retrieve the aggregated result for that stage. Use {@link
     * AggregateBuilder#tag0() builder.tag0()} as the tag of this stage. You
     * will also be able to supply a function to the builder that immediately
     * transforms the results to the desired output type.
     * <p>
     * This example defines a 1-second sliding window and counts the items in
     * stage-0, sums those in stage-1 and takes the average of those in
     * stage-2:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * StreamStage<Long> stage0 = p.drawFrom(source0);
     * StreamStage<Long> stage1 = p.drawFrom(source1);
     * StreamStage<Long> stage2 = p.drawFrom(source2);
     * WindowAggregateBuilder<Long> b = stage0
     *         .window(sliding(1000, 10))
     *         .aggregateBuilder(AggregateOperations.counting());
     * Tag<Long> tag0 = b.tag0();
     * Tag<Long> tag1 = b.add(stage1,
     *         AggregateOperations.summingLong(Long::longValue));
     * Tag<Double> tag2 = b.add(stage2,
     *         AggregateOperations.averagingLong(Long::longValue));
     * StreamStage<TimestampedItem<ItemsByTag>> aggregated = b.build();
     * aggregated.map(e -> String.format(
     *         "Timestamp %d, count of stage0: %d, sum of stage1: %d, average of stage2: %f",
     *         e.timestamp(), e.item().get(tag0), e.item().get(tag1), e.item().get(tag2))
     * );
     *}</pre>
     */
    @Nonnull
    default <R0> WindowAggregateBuilder<R0> aggregateBuilder(
            AggregateOperation1<? super T, ?, ? extends R0> aggrOp
    ) {
        return new WindowAggregateBuilder<>(streamStage(), aggrOp, windowDefinition());
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get.
     * <p>
     * This builder requires you to provide a multi-input aggregate operation.
     * If you can express your logic in terms of single-input aggregate
     * operations, one for each input stream, then you should use {@link
     * #aggregateBuilder(AggregateOperation1) stage0.aggregateBuilder(aggrOp0)}
     * because it offers a simpler API. Use this builder only when you have the
     * need to implement an aggregate operation that combines all the input
     * streams into the same accumulator.
     * <p>
     * This builder is mainly intended to build a co-aggregation of four or
     * more contributing stages. For up to three stages, prefer the direct
     * {@code stage.aggregateN(...)} calls because they offer more static type
     * safety.
     * <p>
     * To add the other stages, call {@link WindowAggregateBuilder1#add
     * add(stage)}. Collect all the tags returned from {@code add()} and use
     * them when building the aggregate operation. Retrieve the tag of the
     * first stage (from which you obtained this builder) by calling {@link
     * WindowAggregateBuilder1#tag0()}.
     * <p>
     * This example takes three streams of strings, specifies a 1-second
     * sliding window and counts the distinct strings across all streams:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * StreamStage<String> stage0 = p.drawFrom(source0);
     * StreamStage<String> stage1 = p.drawFrom(source1);
     * StreamStage<String> stage2 = p.drawFrom(source2);
     * WindowAggregateBuilder1<String> b = stage0
     *         .window(sliding(1000, 10))
     *         .aggregateBuilder();
     * Tag<String> tag0 = b.tag0();
     * Tag<String> tag1 = b.add(stage1);
     * Tag<String> tag2 = b.add(stage2);
     * StreamStage<TimestampedItem<Integer>> aggregated = b.build(AggregateOperation
     *         .withCreate(HashSet<String>::new)
     *         .andAccumulate(tag0, (acc, item) -> acc.add(item))
     *         .andAccumulate(tag1, (acc, item) -> acc.add(item))
     *         .andAccumulate(tag2, (acc, item) -> acc.add(item))
     *         .andCombine(HashSet::addAll)
     *         .andFinish(HashSet::size));
     * }</pre>
     */
    @Nonnull
    default WindowAggregateBuilder1<T> aggregateBuilder() {
        return new WindowAggregateBuilder1<>(streamStage(), windowDefinition());
    }
}
