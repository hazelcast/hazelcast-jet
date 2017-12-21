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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;

public class GroupTransform<T, K, A, R, OUT> implements UnaryTransform<T, OUT> {
    @Nonnull
    private final DistributedFunction<? super T, ? extends K> keyFn;
    @Nonnull
    private final AggregateOperation1<? super T, A, ? extends R> aggrOp;
    @Nullable
    private final DistributedToLongFunction<? super T> timestampFn;
    @Nullable
    private final WindowDefinition wDef;

    public GroupTransform(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp,
            @Nullable DistributedToLongFunction<? super T> timestampFn,
            @Nullable WindowDefinition wDef
    ) {
        this.timestampFn = timestampFn;
        this.wDef = wDef;
        this.keyFn = keyFn;
        this.aggrOp = aggrOp;
    }

    public GroupTransform(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        this(keyFn, aggrOp, null, null);
    }

    @Nonnull
    public DistributedFunction<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Nonnull
    public AggregateOperation1<? super T, A, ? extends R> aggregateOperation() {
        return aggrOp;
    }

    @Nullable
    public DistributedToLongFunction<? super T> timestampFn() {
        return timestampFn;
    }

    @Nullable
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Override
    public String name() {
        return "group-and-aggregate";
    }

    @Override
    public String toString() {
        return name();
    }
}
