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

import com.hazelcast.jet.StageWithGroupingAndTimestamp;
import com.hazelcast.jet.StageWithGroupingAndWindow;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;

/**
 * Javadoc pending.
 */
public class StageWithGroupingAndTimestampImpl<T, K> implements StageWithGroupingAndTimestamp<T, K> {

    final ComputeStageImpl<T> computeStage;
    private final DistributedFunction<? super T, ? extends K> keyFn;
    private final DistributedToLongFunction<? super T> timestampFn;

    StageWithGroupingAndTimestampImpl(
            ComputeStageImpl<T> computeStage,
            DistributedFunction<? super T, ? extends K> keyFn,
            DistributedToLongFunction<? super T> timestampFn
    ) {
        this.computeStage = computeStage;
        this.keyFn = keyFn;
        this.timestampFn = timestampFn;
    }

    @Override
    public DistributedFunction<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Override
    public DistributedToLongFunction<? super T> timestampFn() {
        return timestampFn;
    }

    @Override
    public StageWithGroupingAndWindow<T, K> window(WindowDefinition wDef) {
        return new StageWithGroupingAndWindowImpl<>(computeStage, keyFn, timestampFn, wDef);
    }
}
