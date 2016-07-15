/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collector;

public class GroupingCombinerProcessor<K, V, A, R> implements ContainerProcessor<Tuple2<K, A>, Tuple2<K, R>> {

    private final Map<K, A> cache = new HashMap<>();
    private final Collector<V, A, R> collector;
    private Iterator<Map.Entry<K, A>> finalizationIterator;

    public GroupingCombinerProcessor(Collector<V, A, R> collector) {
        this.collector = collector;
    }

    @Override
    public boolean process(ProducerInputStream<Tuple2<K, A>> inputStream,
                           ConsumerOutputStream<Tuple2<K, R>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple2<K, A> input : inputStream) {
            A value = this.cache.get(input.get0());
            if (value == null) {
                value = collector.supplier().get();
                this.cache.put(input.get0(), value);
            }
            collector.combiner().apply(value, input.get1());
        }
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple2<K, R>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        boolean finalized = false;
        try {
            if (finalizationIterator == null) {
                this.finalizationIterator = this.cache.entrySet().iterator();
            }

            int idx = 0;
            while (this.finalizationIterator.hasNext()) {
                Map.Entry<K, A> next = this.finalizationIterator.next();
                K key = next.getKey();
                R value = collector.finisher().apply(next.getValue());
                outputStream.consume(new JetTuple2<>(key, value));
                if (idx == processorContext.getConfig().getChunkSize() - 1) {
                    break;
                }
                idx++;
            }
            finalized = !this.finalizationIterator.hasNext();
        } finally {
            if (finalized) {
                this.finalizationIterator = null;
                this.cache.clear();
            }
        }
        return finalized;
    }
}
