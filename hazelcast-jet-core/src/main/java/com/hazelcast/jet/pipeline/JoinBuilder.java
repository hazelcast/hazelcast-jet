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

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.impl.PStreamImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Javadoc pending.
 */
public class JoinBuilder<K> {
    private final Map<TupleKey<?>, StreamWithKeyExtractor<?, K>> streams = new HashMap<>();

    public <E> TupleKey<E> add(PStream<E> s, DistributedFunction<E, K> keyF) {
        TupleKey<E> k = new TupleKey<>(streams.size());
        streams.put(k, new StreamWithKeyExtractor<>(s, keyF));
        return k;
    }

    public PStream<KeyedTuple> build() {
        return new PStreamImpl<>();
    }

    private static class StreamWithKeyExtractor<E, K> {
        PStream<E> s;
        DistributedFunction<E, K> extractKeyF;

        StreamWithKeyExtractor(PStream<E> s, DistributedFunction<E, K> keyF) {
            this.s = s;
            this.extractKeyF = keyF;
        }
    }
}
