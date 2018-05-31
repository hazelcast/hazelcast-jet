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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.function.Function;

/**
 * Represents an intermediate step when constructing a group-and-aggregate
 * pipeline stage. This is the base type for the batch and stream variants.
 *
 * @param <T> type of the stream item
 * @param <K> type of the grouping key
 */
public interface GeneralStageWithGrouping<T, K> {

    /**
     * Returns the function that extracts the grouping key from stream items.
     * This function will be used in the aggregating stage you are about to
     * construct using this object.
     */
    @Nonnull
    DistributedFunction<? super T, ? extends K> keyFn();

    /**
     * TODO [viliam] pending javadoc
     *
     * Context is created for each key at the time when an item with that key
     * occurs for the first time. A context, once created, is only released at the
     * end of the job.
     *
     * Saved to snapshot.
     *
     * @param contextFactory
     * @param mapFn
     * @param <C>
     * @param <R>
     * @return
     */
    @Nonnull
    <C, R> GeneralStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    );

    /**
     * TODO [viliam] pending javadoc
     * @param contextFactory
     * @param filterFn
     * @param <C>
     * @return
     */
    @Nonnull
    <C> GeneralStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    );

    /**
     * TODO [viliam] pending javadoc
     *
     * @param contextFactory
     * @param flatMapFn
     * @param <C>
     * @param <R>
     * @return
     */
    @Nonnull
    <C, R> GeneralStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * TODO [viliam] pending javadoc
     *
     * @param aggrOp
     * @param <R>
     * @return
     */
    @Nonnull
    default <R> GeneralStage<Entry<K, R>> rollingAggregation(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return rollingAggregation(aggrOp, Util::entry);
    }

    /**
     * TODO [viliam] pending javadoc
     *
     * @param aggrOp
     * @param mapToOutputFn
     * @param <R>
     * @param <OUT>
     * @return
     */
    @Nonnull
    default <R, OUT> GeneralStage<OUT> rollingAggregation(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<K, R, OUT> mapToOutputFn
    ) {
        // Early check for identity finish: tries to finish an empty accumulator, different instance
        // must be returned.
        Object emptyAcc = aggrOp.createFn().get();
        if (emptyAcc == ((Function) aggrOp.finishFn()).apply(emptyAcc)) {
            throw new IllegalArgumentException("Aggregate operation must not use identity finish");
        }
        AggregateOperation1<? super T, Object, R> aggrOp1 = (AggregateOperation1<? super T, Object, R>) aggrOp;
        DistributedFunction<? super T, ? extends K> keyFnLocal = keyFn();

        return mapUsingContext(ContextFactory.withCreateFn(jet -> aggrOp1.createFn().get()),
                (Object acc, T item) -> {
                    aggrOp1.accumulateFn().accept(acc, item);
                    R r = aggrOp1.finishFn().apply(acc);
                    assert r != acc : "Aggregate operation must not use identity finish";
                    return mapToOutputFn.apply(keyFnLocal.apply(item), r);
                });
    }
}
