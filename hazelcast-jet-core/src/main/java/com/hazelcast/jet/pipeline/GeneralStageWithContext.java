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
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;

import javax.annotation.Nonnull;

/**
 * Represents an intermediate step when constructing a transform-with-context
 * pipeline stage. This is the base type for the batch and stream variants.
 *
 * @param <T> type of the input item
 * @param <C> type of the context object
 */
public interface GeneralStageWithContext<T, C> {

    /**
     * Attaches to this stage a map-with-context stage, one which applies the
     * supplied function to each input item using the context and emits the
     * function's result as the output item. If the result is {@code null}, it
     * emits nothing. Therefore this stage can be used to implement filtering
     * semantics as well.
     * <p>
     * The function parameters are context and item:
     * <pre>
     *     stage.map((context, item) -> /* mapping code &#42;/)
     * </pre>
     * <p>
     * Even though you can use the context object to store runtime state, it
     * won't be saved to state snapshot. It might be useful in batch or
     * non-snapshotted jobs.
     *
     * @param mapFn a stateless mapping function
     * @param <R> the result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> map(@Nonnull DistributedBiFunction<? super C, ? super T, R> mapFn);

    /**
     * Attaches to this stage a filter-with-context stage, one which applies the
     * provided predicate function to each input item using a context to decide
     * whether to pass the item to the output or to discard it. Returns the
     * newly attached stage.
     * <p>
     * The function parameters are context and item:
     * <pre>
     *     stage.filter((context, item) -> /* predicate code &#42;/)
     * </pre>
     * <p>
     * Even though you can use the context object to store runtime state, it
     * won't be saved to state snapshot. It might be useful in batch or
     * non-snapshotted jobs.
     *
     * @param filterFn a stateless filter predicate function
     * @return the newly attached stage
     */
    @Nonnull
    GeneralStage<T> filter(@Nonnull DistributedBiPredicate<? super C, ? super T> filterFn);

    /**
     * Attaches to this stage a flat-map-with-context stage, one which applies
     * the supplied function to each input item using the context and emits all
     * the items from the {@link Traverser} it returns. The traverser must be
     * <em>null-terminated</em>.
     * <p>
     * The function parameters are context and item:
     * <pre>
     *     stage.flatMap((context, item) -> /* traverser creation code &#42;/)
     * </pre>
     * <p>
     * Even though you can use the context object to store runtime state, it
     * won't be saved to state snapshot. It might be useful in batch or
     * non-snapshotted jobs.
     *
     * @param flatMapFn a stateless flatmapping function, whose result type is
     *                  Jet's {@link Traverser}
     * @param <R> the type of items in the result's traversers
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> flatMap(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<R>> flatMapFn
    );
}
