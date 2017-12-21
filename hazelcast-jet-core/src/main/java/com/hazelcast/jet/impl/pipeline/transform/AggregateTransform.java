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
import com.hazelcast.jet.function.DistributedToLongFunction;

public class AggregateTransform<T, A, R, OUT> implements UnaryTransform<T, OUT> {
    private final DistributedToLongFunction<? super T> timestampFn;
    private final WindowDefinition wDef;
    private final AggregateOperation1<? super T, A, ? extends R> aggrOp;

    public AggregateTransform(
            DistributedToLongFunction<? super T> timestampFn,
            WindowDefinition wDef,
            AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        this.wDef = wDef;
        this.timestampFn = timestampFn;
        this.aggrOp = aggrOp;
    }

    public DistributedToLongFunction<? super T> timestampFn() {
        return timestampFn;
    }

    public WindowDefinition windowDefinition() {
        return wDef;
    }

    public AggregateOperation1<? super T, A, ? extends R> aggregateOperation() {
        return aggrOp;
    }

    @Override
    public String name() {
        return "aggregate";
    }

    @Override
    public String toString() {
        return name();
    }
}
