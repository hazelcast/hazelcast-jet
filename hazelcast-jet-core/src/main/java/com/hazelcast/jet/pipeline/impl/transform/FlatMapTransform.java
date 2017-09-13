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

package com.hazelcast.jet.pipeline.impl.transform;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedFunction;

public class FlatMapTransform<E, R> implements UnaryTransform<E, R> {
    private final DistributedFunction<? super E, Traverser<? extends R>> flatMapFn;

    public FlatMapTransform(DistributedFunction<? super E, Traverser<? extends R>> flatMapFn) {
        this.flatMapFn = flatMapFn;
    }

    @Override
    public String toString() {
        return "FlatMap";
    }

    public DistributedFunction<? super E, Traverser<? extends R>> flatMapFn() {
        return flatMapFn;
    }
}
