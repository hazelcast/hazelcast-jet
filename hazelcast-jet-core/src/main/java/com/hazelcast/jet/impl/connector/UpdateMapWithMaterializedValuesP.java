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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.QueryableEntry;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

public class UpdateMapWithMaterializedValuesP<T, K, V> extends AbstractUpdateMapP<T, K, V> {

    private final FunctionEx<? super T, ? extends V> valueFn;

    public UpdateMapWithMaterializedValuesP(
            @Nonnull HazelcastInstance instance,
            @Nonnull String map,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull FunctionEx<? super T, ? extends V> valueFn
    ) {
        super(instance, MAX_PARALLEL_ASYNC_OPS_DEFAULT, map, keyFn);
        this.valueFn = valueFn;
    }


    @Override
    protected EntryProcessor<K, V, Void> entryProcessor(Map<Data, Object> buffer) {
        return new ApplyValuesEntryProcessor<>(buffer);
    }

    @Override
    protected void addToBuffer(T item) {
        K key = keyFn.apply(item);
        boolean shouldBeDropped = shouldBeDropped(key, item);
        if (shouldBeDropped) {
            pendingItemCount--;
            return;
        }

        Data keyData = serializationContext.toKeyData(key);
        int partitionId = serializationContext.partitionId(keyData);

        Map<Data, Object> buffer = partitionBuffers[partitionId];
        Data value = serializationContext.toData(valueFn.apply(item));
        if (buffer.put(keyData, value) == null) {
            pendingInPartition[partitionId]++;
        } else { // item already exists, it will be coalesced
            pendingItemCount--;
        }
    }

    protected boolean shouldBeDropped(K key, T item) {
        return false;
    }

    public static class ApplyValuesEntryProcessor<K, V>
            implements EntryProcessor<K, V, Void>, IdentifiedDataSerializable {

        private Map<Data, Object> keysToUpdate;

        public ApplyValuesEntryProcessor() { //needed for (de)serialization
        }

        public ApplyValuesEntryProcessor(Map<Data, Object> keysToUpdate) {
            this.keysToUpdate = keysToUpdate;
        }

        @Override
        public Void process(Map.Entry<K, V> entry) {
            // avoid re-serialization
            QueryableEntry<Data, Object> e = ((QueryableEntry<Data, Object>) entry);
            e.setValue(keysToUpdate.get(e.getKeyData()));
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(keysToUpdate);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            keysToUpdate = in.readObject();
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
}
