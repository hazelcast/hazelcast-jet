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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.bag.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

/**
 * Contains primitives needed to compute an aggregated result of
 * stream processing. The result is computed by maintaining a mutable
 * result container, called the <em>accumulator</em>, which is transformed
 * to the final result at the end of accumulation. The data items may come
 * from one or more inbound streams; there is a separate {@code accumulate}
 * function for each of them.
 * <ol><li>
 *     {@link #createAccumulatorF() create} a new accumulator object
 * </li><li>
 *     {@link #accumulateItemF() accumulate} the data of an item by mutating
 *     the accumulator
 * </li><li>
 *     {@link #combineAccumulatorsF() combine} the contents of the right-hand
 *     accumulator into the left-hand one
 * </li><li>
 *     {@link #deductAccumulatorF() deduct} the contents of the right-hand
 *     accumulator from the left-hand one (undo the effects of {@code combine})
 * </li><li>
 *     {@link #finishAccumulationF() finish} accumulation by transforming the
 *     accumulator's intermediate result into the final result
 * </li></ol>
 * The <em>deduct</em> primitive is optional. It is used in sliding window
 * aggregation, where it can significantly improve the performance.
 * <h3>Static type design</h3>
 * This interface covers the fully general case where contributing streams
 * are identified by <em>tags</em>. Since there can be any number of
 * streams, this interface has no type parameter for the stream item. The
 * tags themselves carry the type information, but there must be a runtime
 * check whether a given tag is registered with this aggregate operation.
 * <p>
 * There are specializations of this interface to up to three contributing
 * streams whose type is statically captured by this aggregate operation.
 * They are {@link AggregateOperation1}, {@link AggregateOperation2} and
 * {@link AggregateOperation3}. {@link AggregateOperationBuilder} will
 * automatically return the appropriate specialization, depending on which
 * {@code accumulate} primitives are provided to it.
 *
 * @param <T> the type of the stream item &mdash; contravariant
 * @param <A> the type of the accumulator &mdash; invariant
 * @param <R> the type of the final result &mdash; covariant
 */
public interface AggregateOperation<A, R> extends Serializable {

    /**
     * A primitive that returns a new accumulator. If the {@code deduct}
     * operation is defined, the accumulator object must properly implement
     * {@code equals()}, which will be used to detect when an accumulator is
     * "empty" (i.e., equal to a fresh instance returned from this method) and
     * can be evicted from a processor's storage.
     */
    @Nonnull
    DistributedSupplier<A> createAccumulatorF();

    /**
     * A primitive that updates the accumulator state to account for a new
     * item. The tag argument identifies which of the contributing streams
     * in a co-group operation the returned function will handle.
     */
    @Nonnull
    <T> DistributedBiConsumer<? super A, T> accumulateItemF(Tag<T> tag);

    /**
     * A primitive that accepts two accumulators and updates the state of the
     * left-hand one by combining it with the state of the right-hand one.
     * The right-hand accumulator remains unchanged.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF();

    /**
     * A primitive that accepts two accumulators and updates the state of the
     * left-hand one by deducting the state of the right-hand one from it. The
     * right-hand accumulator remains unchanged.
     * <p>
     * The effect of this primitive must be the opposite of {@link
     * #combineAccumulatorsF() combine} so that
     * <pre>
     *     combine(acc, x);
     *     deduct(acc, x);
     * </pre>
     * leaves {@code acc} in the same state as it was before the two
     * operations.
     * <p>
     * <strong>Note:</strong> this method may return {@code null} because the
     * <em>deduct</em> primitive is optional. However, when this aggregate
     * operation is used to compute a sliding window, its presence may
     * significantly reduce the computational cost. With it, the current
     * sliding window can be obtained from the previous one by deducting the
     * trailing frame and combining the leading frame; without it, each window
     * must be recomputed from all its constituent frames. The finer the sliding
     * step, the more pronounced the difference in computation effort will be.
     */
    @Nullable
    DistributedBiConsumer<? super A, ? super A> deductAccumulatorF();

    /**
     * A primitive that finishes the accumulation process by transforming
     * the accumulator object into the final result.
     */
    @Nonnull
    DistributedFunction<? super A, R> finishAccumulationF();

    /**
     * Returns a copy of this aggregate operation with the map of
     * {@code accumulate} primitives replaced by the supplied one.
     */
    @Nonnull
    AggregateOperation<A, R> withAccumulatorsByTag(
            @Nonnull Map<Tag, DistributedBiConsumer<? super A, ?>> accumulatorsByTag);

    @Nonnull
    static <A> AggregateOperationBuilder.Step1<A> withCreate(DistributedSupplier<A> createAccumulatorF) {
        return new AggregateOperationBuilder.Step1<>(createAccumulatorF);
    }
}
