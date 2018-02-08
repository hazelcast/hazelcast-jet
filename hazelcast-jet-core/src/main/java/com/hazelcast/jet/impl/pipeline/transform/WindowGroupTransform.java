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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

public class WindowGroupTransform<T, K, A, R, OUT> extends AbstractTransform implements Transform {
    @Nonnull
    private DistributedFunction<? super T, ? extends K> keyFn;
    @Nonnull
    private AggregateOperation1<? super T, A, R> aggrOp;
    @Nonnull
    private final KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn;
    @Nonnull
    private final WindowDefinition wDef;

    public WindowGroupTransform(
            @Nonnull Transform upstream,
            @Nonnull WindowDefinition wDef,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        super("group-and-aggregate", upstream);
        this.wDef = wDef;
        this.keyFn = keyFn;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }
;
    @Nonnull
    public DistributedFunction<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Nonnull
    public AggregateOperation1<? super T, A, R> aggrOp() {
        return aggrOp;
    }

    @Nonnull
    public KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn() {
        return mapToOutputFn;
    }

    @Nonnull
    public WindowDefinition wDef() {
        return wDef;
    }
}
