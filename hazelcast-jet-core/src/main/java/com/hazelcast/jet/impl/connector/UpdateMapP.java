/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import static com.hazelcast.util.MapUtil.createHashMap;

public final class UpdateMapP<T, K, V> extends AsyncHazelcastWriterP {

    private final String mapName;
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    private final BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
    private final Consumer<T> addToBuffer = this::addToBuffer;

    private IPartitionService memberPartitionService;
    private ClientPartitionService clientPartitionService;
    private SerializationService serializationService;
    private IMap<K, V> map;

    // one map per partition to store the temporary values
    private Map<Data, Object>[] tmpMaps;

    private UpdateMapP(HazelcastInstance instance, boolean isLocal,
                       String mapName,
                       @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                       @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        super(instance, isLocal);
        this.mapName = mapName;
        this.toKeyFn = toKeyFn;
        this.updateFn = updateFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        map = instance().getMap(mapName);
        int partitionCount;
        if (isLocal()) {
            HazelcastInstanceImpl castedInstance = (HazelcastInstanceImpl) instance();
            clientPartitionService = null;
            memberPartitionService = castedInstance.node.nodeEngine.getPartitionService();
            serializationService = castedInstance.getSerializationService();
            partitionCount = memberPartitionService.getPartitionCount();
        } else {
            HazelcastClientProxy clientProxy = (HazelcastClientProxy) instance();
            clientPartitionService = clientProxy.client.getClientPartitionService();
            memberPartitionService = null;
            serializationService = clientProxy.getSerializationService();
            partitionCount = clientPartitionService.getPartitionCount();
        }
        tmpMaps = new Map[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            tmpMaps[i] = new HashMap<>();
        }
    }

    @Override
    protected void processInternal(Inbox inbox) {
        //TODO: we may consider not draining here if tmpMaps is too big?
        inbox.drain(addToBuffer);
        submit();
    }

    private void submit() {
        for (int partitionId = 0; partitionId < tmpMaps.length; partitionId++) {
            if (tmpMaps[partitionId].isEmpty()) {
                continue;
            }
            if (!acquirePermit()) {
                return;
            }

            ApplyFnEntryProcessor<K, V, T> entryProcessor =
                new ApplyFnEntryProcessor<>(tmpMaps[partitionId], updateFn);
            setCallback(submitToKeys(map, tmpMaps[partitionId].keySet(), entryProcessor));
            tmpMaps[partitionId] = new HashMap<>();
        }
    }

    private void addToBuffer(T item) {
        K key = toKeyFn.apply(item);
        int partitionId;
        Data keyData;
        if (isLocal()) {
            // We pre-serialize the key and value to avoid double serialization when partitionId
            // is calculated and when the value for backup operation is re-serialized
            keyData = serializationService.toData(key, ((MapProxyImpl) map).getPartitionStrategy());
            partitionId = memberPartitionService.getPartitionId(keyData);
        } else {
            // We ignore partition strategy for remote connection, the client doesn't know it.
            // TODO we might be able to fix this after https://github.com/hazelcast/hazelcast/issues/13950 is fixed
            // The functionality should work, but will be ineffective: the submitOnKey calls will have wrongly
            // partitioned data.
            keyData = serializationService.toData(key);
            partitionId = clientPartitionService.getPartitionId(keyData);
        }
        Data itemData = serializationService.toData(item);
        tmpMaps[partitionId].merge(keyData, itemData, (o, n) -> ApplyFnEntryProcessor.append(o, (Data) n));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> ICompletableFuture<Map<K, V>> submitToKeys(
        IMap<K, V> map, Set<Data> keys, EntryProcessor<K, V> entryProcessor) {
        // TODO remove this method once submitToKeys is public API
        // we force Set<Data> instead of Set<K> to avoid re-serialization of keys
        // this relies on an implementation detail of submitToKeys method.
        if (map instanceof MapProxyImpl) {
            return ((MapProxyImpl) map).submitToKeys(keys, entryProcessor);
        } else if (map instanceof ClientMapProxy) {
            return ((ClientMapProxy) map).submitToKeys(keys, entryProcessor);
        } else {
            throw new RuntimeException("Unexpected map class: " + map.getClass().getName());
        }
    }

    static class Supplier<T, K, V> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private String name;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final BiFunctionEx<? super V, ? super T, ? extends V> updateFn;

        Supplier(@Nullable String clientXml,
                 @Nonnull String name,
                 @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                 @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
        ) {
            super(clientXml);
            this.name = name;
            this.toKeyFn = toKeyFn;
            this.updateFn = updateFn;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, boolean isLocal) {
            return new UpdateMapP<>(instance, isLocal, name, toKeyFn, updateFn);
        }
    }

    @SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized")
    public static class ApplyFnEntryProcessor<K, V, T>
        implements EntryProcessor<K, V>, EntryBackupProcessor<K, V>, IdentifiedDataSerializable,
        SerializationServiceAware {
        private Map<Data, Object> keysToUpdate;
        private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
        private SerializationService serializationService;

        public ApplyFnEntryProcessor() {
        }

        ApplyFnEntryProcessor(
            Map<Data, Object> keysToUpdate,
            BiFunctionEx<? super V, ? super T, ? extends V> updateFn
        ) {
            this.keysToUpdate = keysToUpdate;
            this.updateFn = updateFn;
        }

        @Override
        public Object process(Entry<K, V> entry) {
            // it should not matter that we don't take the PartitionStrategy here into account
            Data keyData = serializationService.toData(entry.getKey());
            Object item = keysToUpdate.get(keyData);
            if (item == null && !keysToUpdate.containsKey(keyData)) {
                // Implementing equals/hashCode is not required for IMap keys since serialized version is used
                // instead. After serializing/deserializing the keys they will have different identity. And since they
                // don't implement the methods, they key can't be found in the map.
                throw new JetException("The new item not found in the map - is equals/hashCode " +
                    "correctly implemented for the key? Key type: " + entry.getKey().getClass().getName());
            }
            if (item instanceof List) {
                for (Data o : ((List<Data>) item)) {
                    handle(entry, o);
                }
            } else {
                handle(entry, (Data) item);
            }
            return null;
        }

        private void handle(Entry<K, V> entry, Data itemData) {
            T item = serializationService.toObject(itemData);
            V oldValue = entry.getValue();
            V newValue = updateFn.apply(oldValue, item);
            entry.setValue(newValue);
        }

        @Override
        public EntryBackupProcessor<K, V> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Entry<K, V> entry) {
            process(entry);
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(keysToUpdate.size());
            for (Entry<Data, Object> en : keysToUpdate.entrySet()) {
                out.writeData(en.getKey());
                Object value = en.getValue();
                if (value instanceof Data) {
                    out.writeInt(1);
                    out.writeData((Data) value);
                } else if (value instanceof List) {
                    List<Data> list = (List<Data>) value;
                    out.writeInt(list.size());
                    for (Data data : list) {
                        out.writeData(data);
                    }
                } else {
                    assert false : "Unknown value type: " + value.getClass();
                }
            }
            out.writeObject(updateFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int keysToUpdateSize = in.readInt();
            keysToUpdate = createHashMap(keysToUpdateSize);
            for (int i = 0; i < keysToUpdateSize; i++) {
                Data key = in.readData();
                int size = in.readInt();
                Object value;
                if (size == 1) {
                    value = in.readData();
                } else {
                    List<Data> list = new ArrayList<>(size);
                    for (int j = 0; j < size; j++) {
                        list.add(in.readData());
                    }
                    value = list;
                }
                keysToUpdate.put(key, value);
            }
            updateFn = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetDataSerializerHook.APPLY_FN_ENTRY_PROCESSOR;
        }

        // used to group entries when more than one entry exists for the same key
        public static Object append(Object value, Data item) {
            List<Data> list;
            if (value instanceof List) {
                list = (List) value;
            } else {
                list = new ArrayList<>();
                list.add((Data) value);
            }
            list.add(item);
            return list;
        }
    }

}
