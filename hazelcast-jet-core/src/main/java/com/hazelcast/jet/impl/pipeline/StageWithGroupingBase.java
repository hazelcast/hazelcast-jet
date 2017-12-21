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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
class StageWithGroupingBase<T, K> {

    @Nonnull
    final ComputeStageImpl<T> computeStage;
    @Nonnull
    private final DistributedFunction<? super T, ? extends K> keyFn;
    @Nullable
    private final DistributedToLongFunction<? super T> timestampFn;
    @Nullable
    private final WindowDefinition wDef;

    StageWithGroupingBase(
            @Nonnull ComputeStageImpl<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nullable DistributedToLongFunction<? super T> timestampFn,
            @Nullable WindowDefinition wDef
    ) {
        this.computeStage = computeStage;
        this.keyFn = keyFn;
        this.timestampFn = timestampFn;
        this.wDef = wDef;
    }

    @Nonnull
    public DistributedFunction<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Nonnull
    public DistributedToLongFunction<? super T> timestampFn() {
        return timestampFn;
    }

    @Nonnull
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Nonnull
    ComputeStageImpl<T> computeStage() {
        return computeStage;
    }
}
