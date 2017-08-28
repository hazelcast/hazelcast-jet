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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.impl.transform.FilterTransform;
import com.hazelcast.jet.pipeline.impl.transform.FlatMapTransform;
import com.hazelcast.jet.pipeline.impl.transform.GroupByTransform;
import com.hazelcast.jet.pipeline.impl.transform.MapTransform;
import com.hazelcast.jet.pipeline.impl.transform.ProcessorTransform;
import com.hazelcast.jet.pipeline.impl.transform.SlidingWindowTransform;

import java.util.Map.Entry;

public final class Transforms {

    private Transforms() {
    }

    public static <E, R> UnaryTransform<E, R> fromProcessor(
            String transforName, DistributedSupplier<Processor> procSupplier
    ) {
        return new ProcessorTransform<>(transforName, procSupplier);
    }

    public static <E, R> UnaryTransform<E, R> map(DistributedFunction<? super E, ? extends R> mapF) {
        return new MapTransform<>(mapF);
    }

    public static <E, R> UnaryTransform<E, R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapF) {
        return new FlatMapTransform<>(flatMapF);
    }

    public static <E> UnaryTransform<E, E> filter(DistributedPredicate<? super E> filterF) {
        return new FilterTransform<>(filterF);
    }

    public static <E, K, R> UnaryTransform<E, Entry<K, R>> groupBy(
            DistributedFunction<? super E, ? extends K> keyF,
            AggregateOperation1<E, ?, R> aggregation
    ) {
        return new GroupByTransform<>(keyF, aggregation);
    }

    public static <IN, K, R> UnaryTransform<IN, Entry<K, R>> slidingWindow(
            DistributedFunction<IN, K> keyF,
            WindowDefinition wDef,
            AggregateOperation1<IN, ?, R> aggregation
    ) {
        return new SlidingWindowTransform<>(keyF, wDef, aggregation);
    }


}
