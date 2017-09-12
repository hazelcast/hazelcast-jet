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

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.UnaryTransform;

/**
 * Javadoc pending.
 */
public class MapTransform<T, R> implements UnaryTransform<T, R> {
    public final DistributedFunction<? super T, ? extends R> mapFn;

    public MapTransform(DistributedFunction<? super T, ? extends R> mapFn) {
        this.mapFn = mapFn;
    }

    @Override
    public String toString() {
        return "Map";
    }
}
