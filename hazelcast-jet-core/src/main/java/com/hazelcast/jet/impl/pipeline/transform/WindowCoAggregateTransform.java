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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class WindowCoAggregateTransform<A, R, OUT> extends AbstractTransform implements Transform {
    @Nonnull
    private AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final WindowDefinition wDef;
    @Nonnull
    private final WindowResultFunction<? super R, ? extends OUT> mapToOutputFn;

    public WindowCoAggregateTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull WindowDefinition wDef,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        super(upstream.size() + "-way co-aggregate", upstream);
        this.aggrOp = aggrOp;
        this.wDef = wDef;
        this.mapToOutputFn = mapToOutputFn;
    }

    @Nonnull
    public AggregateOperation<A, R> aggrOp() {
        return aggrOp;
    }

    @Nonnull
    public WindowDefinition wDef() {
        return wDef;
    }

    @Nonnull
    public WindowResultFunction<? super R, ? extends OUT> mapToOutputFn() {
        return mapToOutputFn;
    }
}
