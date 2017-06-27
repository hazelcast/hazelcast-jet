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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.impl.FlatmapTransform;
import com.hazelcast.jet.pipeline.impl.GroupByTransform;
import com.hazelcast.jet.pipeline.impl.MapTransform;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;
import com.hazelcast.jet.pipeline.impl.SlidingWindowTransform;

import java.util.Map;

public final class Transforms {

    private Transforms() {

    }

    public static <E, R> Transform<E, R> map(DistributedFunction<? super E, ? extends R> mapF) {
        return new MapTransform<>(mapF);
    }

    public static <E, R> Transform<E, R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapF) {
        return new FlatmapTransform<>(flatMapF);
    }

    public static <E, K, R> Transform<E, Map.Entry<K, R>> groupBy(
            DistributedFunction<? super E, ? extends K> keyF,
            AggregateOperation<E, ?, R> aggregation
    ) {
        return new GroupByTransform<>(keyF, aggregation);
    }

    public static <IN, K, R> Transform<IN, Map.Entry<K, R>> slidingWindow(
            DistributedFunction<IN, K> keyF,
            WindowDefinition wDef,
            AggregateOperation<IN, ?, R> aggregation
    ) {
        return new SlidingWindowTransform<>(keyF, wDef, aggregation);
    }
}
