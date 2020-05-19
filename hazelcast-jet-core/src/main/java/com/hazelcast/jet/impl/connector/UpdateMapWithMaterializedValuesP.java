/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

public final class UpdateMapWithMaterializedValuesP<T, K, V> extends AbstractUpdateMapP<T, K, V, V> {

    private final FunctionEx<? super T, ? extends K> keyFn;
    private final FunctionEx<? super T, ? extends V> valueFn;

    UpdateMapWithMaterializedValuesP(HazelcastInstance instance,
                                     int maxParallelAsyncOps,
                                     String mapName,
                                     @Nonnull FunctionEx<? super T, ? extends K> keyFn,
                                     @Nonnull FunctionEx<? super T, ? extends V> valueFn) {
        super(instance, maxParallelAsyncOps, mapName);
        this.keyFn = keyFn;
        this.valueFn = valueFn;
    }

    @Override
    protected EntryProcessor<K, V, Object> entryProcessor(Map<K, V> buffer) {
        return new ApplyMaterializedValuesEntryProcessor<>(buffer);
    }

    @Override
    protected void addToBuffer(T item) {
        K key = keyFn.apply(item);
        V value = valueFn.apply(item);

        int partitionId = env.getPartitionId(key);

        Map<K, V> buffer = partitionBuffers[partitionId];
        if (buffer.containsKey(key)) { //pending items will merge
            pendingItemCount--;
        } else {
            pendingInPartition[partitionId]++;
        }
        buffer.put(key, value);
    }

    public static class ApplyMaterializedValuesEntryProcessor<K, V>
            implements EntryProcessor<K, V, Object>, IdentifiedDataSerializable {

        private Map<K, V> updates;

        public ApplyMaterializedValuesEntryProcessor() { //needed for (de)serialization
        }

        public ApplyMaterializedValuesEntryProcessor(Map<K, V> updates) {
            this.updates = updates;
        }

        @Override
        public Object process(Map.Entry<K, V> entry) {
            K key = entry.getKey();
            V newValue = updates.get(key);
            entry.setValue(newValue);
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(updates);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            updates = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.APPLY_MATERIALIZED_VALUE_ENTRY_PROCESSOR;
        }
    }

    public static class Supplier<T, K, V> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends V> valueFn;

        public Supplier(
                @Nullable String clientXml,
                @Nonnull String name,
                @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                @Nonnull FunctionEx<? super T, ? extends V> valueFn
        ) {
            super(clientXml);
            this.name = name;
            this.toKeyFn = toKeyFn;
            this.valueFn = valueFn;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
            return new UpdateMapWithMaterializedValuesP<T, K, V>(
                    instance, MAX_PARALLEL_ASYNC_OPS_DEFAULT, name, toKeyFn, valueFn
            );
        }
    }
}
