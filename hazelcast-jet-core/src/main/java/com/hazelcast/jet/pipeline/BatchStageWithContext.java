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
 * Represents an intermediate step while constructing a transform-with-context
 * batch pipeline stage. It captures the {@link ContextFactory} key and offers
 * the methods to finalize the construction by specifying the transformation
 * step (map, filter or flatMap).
 *
 * @param <T> type of the input item
 * @param <C> type of the context object
 */
public interface BatchStageWithContext<T, C> extends GeneralStageWithContext<T, C> {

    @Nonnull @Override
    GeneralStage<T> filter(@Nonnull DistributedBiPredicate<? super C, ? super T> filterFn);

    @Nonnull @Override
    <R> BatchStage<R> map(@Nonnull DistributedBiFunction<? super C, ? super T, R> mapFn);

    @Nonnull @Override
    <R> BatchStage<R> flatMap(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<R>> flatMapFn
    );
}
