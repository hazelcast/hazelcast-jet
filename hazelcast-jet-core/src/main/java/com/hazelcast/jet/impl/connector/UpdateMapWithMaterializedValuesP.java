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

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UpdateMapWithMaterializedValuesP<T, K, V> extends AsyncHazelcastWriterP {

    private static final int PENDING_ITEM_COUNT_LIMIT = 1024;

    private final String mapName;
    private final FunctionEx<? super T, ? extends K> keyFn;
    private final FunctionEx<? super T, ? extends V> valueFn;

    private PartitionService partitionService;
    private IMap<K, V> map;

    // one map per partition to store the updates
    private Map<K, V>[] tmpMaps;
    // count how many pending actual items are in each map
    private int[] tmpCounts;

    private int pendingItemCount;
    private int currentPartitionId;

    UpdateMapWithMaterializedValuesP(HazelcastInstance instance,
                                     int maxParallelAsyncOps,
                                     String mapName,
                                     @Nonnull FunctionEx<? super T, ? extends K> keyFn,
                                     @Nonnull FunctionEx<? super T, ? extends V> valueFn) {
        super(instance, maxParallelAsyncOps);
        this.mapName = mapName;
        this.keyFn = keyFn;
        this.valueFn = valueFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        map = instance().getMap(mapName);
        partitionService = new PartitionService();

        int partitionCount = partitionService.getPartitionCount();
        tmpMaps = new Map[partitionCount];
        tmpCounts = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            tmpMaps[i] = new HashMap<>();
        }
    }

    @Override
    protected void processInternal(Inbox inbox) {
        if (pendingItemCount < PENDING_ITEM_COUNT_LIMIT) {
            inbox.drain(this::addToBuffer);
        }
        submitPending();
    }

    @Override
    protected boolean flushInternal() {
        return submitPending();
    }

    // returns if we were able to submit all pending items
    private boolean submitPending() {
        if (pendingItemCount == 0) {
            return true;
        }
        for (int i = 0; i < tmpMaps.length; i++, currentPartitionId = incrCircular(currentPartitionId, tmpMaps.length)) {
            if (tmpMaps[currentPartitionId].isEmpty()) {
                continue;
            }
            if (!tryAcquirePermit()) {
                return false;
            }

            Map<K, V> updates = tmpMaps[currentPartitionId];
            ApplyMaterializedValuesEntryProcessor<K, V> entryProcessor =
                    new ApplyMaterializedValuesEntryProcessor<>(updates);
            setCallback(map.submitToKeys(updates.keySet(), entryProcessor));
            pendingItemCount -= tmpCounts[currentPartitionId];
            tmpCounts[currentPartitionId] = 0;
            tmpMaps[currentPartitionId] = new HashMap<>();
        }
        if (currentPartitionId == tmpMaps.length) {
            currentPartitionId = 0;
        }
        assert pendingItemCount == 0 : "pending item count should be 0, but was " + pendingItemCount;
        return true;
    }

    private void addToBuffer(T item) {
        K key = keyFn.apply(item);
        V value = valueFn.apply(item);

        int partitionId = partitionService.getPartitionId(key);

        Map<K, V> tmpMap = tmpMaps[partitionId];
        if (!tmpMap.containsKey(key)) {
            tmpCounts[partitionId]++;
            pendingItemCount++;
        }
        tmpMap.put(key, value);
    }

    private class PartitionService {

        private final IPartitionService memberPartitionService;
        private final ClientPartitionService clientPartitionService;

        PartitionService() {
            if (isLocal()) {
                HazelcastInstanceImpl castInstance = (HazelcastInstanceImpl) instance();
                clientPartitionService = null;
                memberPartitionService = castInstance.node.nodeEngine.getPartitionService();
            } else {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) instance();
                clientPartitionService = clientProxy.client.getClientPartitionService();
                memberPartitionService = null;
            }
        }

        int getPartitionCount() {
            return isLocal() ? memberPartitionService.getPartitionCount() :
                    clientPartitionService.getPartitionCount();
        }

        public int getPartitionId(Object key) {
            return isLocal() ? memberPartitionService.getPartitionId(key) :
                    clientPartitionService.getPartitionId(key);
        }
    }

    /**
     * Returns {@code v+1} or 0, if {@code v+1 == limit}.
     */
    @CheckReturnValue
    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }

    public static class Supplier<T, K, V> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends V> valueFn;
        private String name;

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
}
