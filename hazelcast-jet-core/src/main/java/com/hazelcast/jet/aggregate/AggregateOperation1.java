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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;

/**
 * Specialization of {@link AggregateOperation} to the "arity-1" case with
 * a single data stream being aggregated over.
 *
 * @param <T> the type of the stream item
 * @param <A> the type of the accumulator
 * @param <R> the type of the aggregation result
 */
public interface AggregateOperation1<T, A, R> extends AggregateOperation<A, R> {

    /**
     * A primitive that updates the accumulator state to account for a new
     * item.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super T> accumulateFn();

    // Override with a narrowed return type
    @Nonnull @Override
    <R_NEW> AggregateOperation1<T, A, R_NEW> withFinishFn(
            @Nonnull DistributedFunction<? super A, R_NEW> finishFn
    );
}
