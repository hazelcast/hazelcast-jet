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

import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class CoGroupTransform<K, A, R, OUT> implements MultaryTransform<OUT> {
    @Nonnull
    private final List<DistributedFunction<?, ? extends K>> groupKeyFns;
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;
    @Nullable
    private final WindowDefinition wDef;

    public CoGroupTransform(
            @Nonnull List<DistributedFunction<?, ? extends K>> groupKeyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nullable WindowDefinition wDef
    ) {
        this.wDef = wDef;
        this.groupKeyFns = groupKeyFns;
        this.aggrOp = aggrOp;
    }

    public CoGroupTransform(
            @Nonnull List<DistributedFunction<?, ? extends K>> groupKeyFns,
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        this(groupKeyFns, aggrOp, null);
    }

    @Nonnull
    public List<DistributedFunction<?, ? extends K>> groupKeyFns() {
        return groupKeyFns;
    }

    @Nonnull
    public AggregateOperation<A, R> aggregateOperation() {
        return aggrOp;
    }

    @Nullable
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Override
    public String name() {
        return groupKeyFns.size() + "-way co-group and aggregate";
    }

    @Override
    public String toString() {
        return name();
    }
}
