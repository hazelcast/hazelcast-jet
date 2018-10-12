/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.SerializationConstants;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.callbackOf;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.tryIncrement;
import static java.util.stream.Collectors.toList;

/**
 * This is private API. Check out the {@link SinkProcessors} class for
 * public factory methods.
 */
public final class HazelcastWriters {

    private HazelcastWriters() {
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier mergeMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
            @Nonnull DistributedFunction<? super T, ? extends V> toValueFn,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toValueFn, "toValueFn");
        checkSerializable(mergeFn, "mergeFn");

        return updateMapSupplier(name, clientConfig, toKeyFn, (V oldValue, T item) -> {
            V newValue = toValueFn.apply(item);
            if (oldValue == null) {
                return newValue;
            }
            return mergeFn.apply(oldValue, newValue);
        });
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String mapName,
            @Nullable ClientConfig clientConfig,
            @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
            @Nonnull DistributedBiFunction<? super V, ? super T, ? extends V> updateFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(updateFn, "updateFn");

        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<UpdateMapContext<K, V, T>, T>(
                serializableConfig(clientConfig),
                instance -> procContext -> new UpdateMapContext<>(instance, mapName, toKeyFn, updateFn, isLocal),
                UpdateMapContext::add,
                instance -> UpdateMapContext::flush,
                UpdateMapContext::finish
        ));
    }

    private static final class UpdateMapContext<K, V, T> {
        private static final int MAX_PARALLEL_ASYNC_OPS = 1000;

        private final DistributedFunction<? super T, ? extends K> toKeyFn;
        private final DistributedBiFunction<? super V, ? super T, ? extends V> updateFn;
        private final boolean isLocal;
        private final PartitionService partitionService;
        private final IPartitionService internalPartitionService;
        private final SerializationService serializationService;

        private final Semaphore concurrentAsyncOpsSemaphore = new Semaphore(MAX_PARALLEL_ASYNC_OPS);
        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        private final IMap map;
        private final Map<K, Object>[] tmpMaps;

        UpdateMapContext(
                HazelcastInstance instance,
                String mapName,
                @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
                @Nonnull DistributedBiFunction<? super V, ? super T, ? extends V> updateFn,
                boolean isLocal) {
            this.toKeyFn = toKeyFn;
            this.updateFn = updateFn;
            this.isLocal = isLocal;

            map = instance.getMap(mapName);
            partitionService = instance.getPartitionService();
            if (isLocal) {
                HazelcastInstanceImpl castedInstance = (HazelcastInstanceImpl) instance;
                internalPartitionService = castedInstance.node.nodeEngine.getPartitionService();
                serializationService = castedInstance.getSerializationService();
            } else {
                internalPartitionService = null;
                serializationService = null;
            }
            int partitionCount = partitionService.getPartitions().size();
            tmpMaps = new Map[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                tmpMaps[i] = new HashMap<>();
            }
        }

        void add(T item) {
            K key = toKeyFn.apply(item);
            // Calculating partitionId requires serializing - we serialize the key twice unnecessarily, but I don't
            // know a way to avoid it.
            Data keyData;
            int partitionId;
            if (isLocal) {
                keyData = serializationService.toData(key, ((MapProxyImpl) map).getPartitionStrategy());
                partitionId = internalPartitionService.getPartitionId(keyData);
            } else {
                // We ignore it partition strategy for remote connection, the client doesn't know of it.
                // The functionality should work, but will be ineffective.
                partitionId = partitionService.getPartition(key).getPartitionId();
            }
            tmpMaps[partitionId].merge(key, item, MultiItem::merge);
        }

        void flush() {
            try {
                if (firstError.get() != null) {
                    if (firstError.get() instanceof HazelcastInstanceNotActiveException) {
                        throw handleInstanceNotActive((HazelcastInstanceNotActiveException) firstError.get(), isLocal);
                    }
                    throw sneakyThrow(firstError.get());
                }
                for (int partitionId = 0; partitionId < tmpMaps.length; partitionId++) {
                    if (tmpMaps[partitionId].isEmpty()) {
                        continue;
                    }
                    ApplyFnEntryProcessor entryProcessor = new ApplyFnEntryProcessor(tmpMaps[partitionId], updateFn);
                    try {
                        // block until we get a permit
                        concurrentAsyncOpsSemaphore.acquire();
                    } catch (InterruptedException e) {
                        return;
                    }
                    submitToKeys(map, tmpMaps[partitionId].keySet(), entryProcessor)
                            .andThen(new ExecutionCallback() {
                                @Override
                                public void onResponse(Object response) {
                                    concurrentAsyncOpsSemaphore.release();
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    firstError.compareAndSet(null, t);
                                    concurrentAsyncOpsSemaphore.release();
                                }
                            });
                    tmpMaps[partitionId] = new HashMap<>();
                }
            } catch (HazelcastInstanceNotActiveException e) {
                throw handleInstanceNotActive(e, isLocal);
            }
        }

        public void finish() {
            try {
                // Acquire all initial permits. These won't be available until all async ops finish.
                concurrentAsyncOpsSemaphore.acquire(MAX_PARALLEL_ASYNC_OPS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private static <K> ICompletableFuture<Map<K, Object>> submitToKeys(
            IMap<K, ?> map, Set<K> keys, EntryProcessor entryProcessor) {
        // TODO remove this method once submitToKeys is public API
        if (map instanceof MapProxyImpl) {
            return ((MapProxyImpl) map).submitToKeys(keys, entryProcessor);
        } else if (map instanceof ClientMapProxy) {
            return ((ClientMapProxy) map).submitToKeys(keys, entryProcessor);
        } else {
            throw new RuntimeException("Unexpected map class: " + map.getClass().getName());
        }
    }

    /**
     * This is just a plain java.util.ArrayList. We extend it in order
     * to distinguish it from other stream items, which could be
     * ArrayList themselves.
     */
    private static class MultiItem<E> extends ArrayList<E> {
        public static <E> MultiItem<E> merge(Object o, E n) {
            MultiItem<E> multiItem;
            if (o instanceof MultiItem) {
                multiItem = (MultiItem<E>) o;
            } else {
                multiItem = new MultiItem<>();
                multiItem.add((E) o);
            }
            multiItem.add(n);
            return multiItem;
        }
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
            @Nonnull DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toEntryProcessorFn, "toEntryProcessorFn");

        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new EntryProcessorWriterSupplier<>(
                        name,
                        serializableConfig(clientConfig),
                        toKeyFn,
                        toEntryProcessorFn,
                        isLocal
                )
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static ProcessorMetaSupplier writeMapSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                instance -> procContext -> new ArrayMap(),
                ArrayMap::add,
                instance -> {
                    IMap map = instance.getMap(name);
                    return buffer -> {
                        try {
                            map.putAll(buffer);
                        } catch (HazelcastInstanceNotActiveException e) {
                            throw handleInstanceNotActive(e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                DistributedConsumer.noop()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeCacheSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                instance -> procContext -> new ArrayMap(),
                ArrayMap::add,
                CacheFlush.flushToCache(name, isLocal),
                DistributedConsumer.noop()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                instance -> procContext -> new ArrayList<>(),
                ArrayList::add,
                instance -> {
                    IList<Object> list = instance.getList(name);
                    return buffer -> {
                        try {
                            list.addAll(buffer);
                        } catch (HazelcastInstanceNotActiveException e) {
                            throw handleInstanceNotActive(e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                DistributedConsumer.noop()
        ));
    }

    private static RuntimeException handleInstanceNotActive(HazelcastInstanceNotActiveException e, boolean isLocal) {
        // if we are writing to a local instance, restarting the job should resolve the error
        return isLocal ? new RestartableException(e) : e;
    }

    private static SerializableClientConfig serializableConfig(ClientConfig clientConfig) {
        return clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
    }

    /**
     * Wrapper class needed to conceal the JCache API while
     * serializing/deserializing other lambdas
     */
    private static class CacheFlush {

        static DistributedFunction<HazelcastInstance, DistributedConsumer<ArrayMap>> flushToCache(
                String name, boolean isLocal
        ) {
            return instance -> {
                ICache cache = instance.getCacheManager().getCache(name);
                return buffer -> {
                    try {
                        cache.putAll(buffer);
                    } catch (HazelcastInstanceNotActiveException e) {
                        throw handleInstanceNotActive(e, isLocal);
                    }
                    buffer.clear();
                };
            };
        }
    }

    private static final class ArrayMap extends AbstractMap<Object, Object> {

        private final List<Entry<Object, Object>> entries;
        private final ArraySet set = new ArraySet();

        ArrayMap() {
            entries = new ArrayList<>();
        }

        @Override @Nonnull
        public Set<Entry<Object, Object>> entrySet() {
            return set;
        }

        public void add(Map.Entry entry) {
            entries.add(entry);
        }

        private class ArraySet extends AbstractSet<Entry<Object, Object>> {
            @Override @Nonnull
            public Iterator<Entry<Object, Object>> iterator() {
                return entries.iterator();
            }

            @Override
            public int size() {
                return entries.size();
            }
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }

    private static final class EntryProcessorWriter<T, K, V> extends AbstractProcessor {

        private static final int MAX_PARALLEL_ASYNC_OPS = 1000;
        private final AtomicInteger numConcurrentOps = new AtomicInteger();

        private final boolean isLocal;
        private final IMap<? super K, ? extends V> map;
        private final DistributedFunction<? super T, ? extends K> toKeyFn;
        private final DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;
        private final AtomicReference<Throwable> lastError = new AtomicReference<>();
        private final ExecutionCallback callback = callbackOf(
                response -> numConcurrentOps.decrementAndGet(),
                exception -> {
                    numConcurrentOps.decrementAndGet();
                    if (exception != null) {
                        lastError.compareAndSet(null, exception);
                    }
                });

        private EntryProcessorWriter(
                @Nonnull HazelcastInstance instance,
                @Nonnull String name,
                @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
                @Nonnull DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
                boolean isLocal
        ) {
            this.map = instance.getMap(name);
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
            this.isLocal = isLocal;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean tryProcess() {
            checkError();
            return true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object object) {
            checkError();
            if (!tryIncrement(numConcurrentOps, 1, MAX_PARALLEL_ASYNC_OPS)) {
                return false;
            }
            try {
                @SuppressWarnings("unchecked")
                T item = (T) object;
                EntryProcessor<K, V> entryProcessor = toEntryProcessorFn.apply(item);
                K key = toKeyFn.apply(item);
                map.submitToKey(key, entryProcessor, callback);
                return true;
            } catch (HazelcastInstanceNotActiveException e) {
                throw handleInstanceNotActive(e, isLocal);
            }
        }

        @Override
        public boolean complete() {
            return ensureAllWritten();
        }

        @Override
        public boolean saveToSnapshot() {
            return ensureAllWritten();
        }

        private boolean ensureAllWritten() {
            boolean allWritten = numConcurrentOps.get() == 0;
            checkError();
            return allWritten;
        }

        private void checkError() {
            Throwable t = lastError.get();
            if (t != null) {
                throw sneakyThrow(t);
            }
        }
    }

    private static final class EntryProcessorWriterSupplier<T, K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final SerializableClientConfig clientConfig;
        private final DistributedFunction<? super T, ? extends K> toKeyFn;
        private final DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;
        private final boolean isLocal;
        private transient HazelcastInstance client;
        private transient HazelcastInstance instance;

        private EntryProcessorWriterSupplier(
                @Nonnull String name,
                @Nullable SerializableClientConfig clientConfig,
                @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
                @Nonnull DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
                boolean isLocal
        ) {
            this.name = name;
            this.clientConfig = clientConfig;
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
            this.isLocal = isLocal;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (clientConfig != null) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() ->
                    new EntryProcessorWriter<>(instance, name, toKeyFn, toEntryProcessorFn, isLocal))
                         .limit(count)
                         .collect(toList());
        }
    }

    private static class HazelcastWriterSupplier<B, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig clientConfig;
        private final DistributedFunction<HazelcastInstance, DistributedConsumer<B>> instanceToFlushBufferFn;
        private final DistributedFunction<HazelcastInstance, DistributedFunction<Processor.Context, B>>
                instanceToNewBufferFn;
        private final DistributedBiConsumer<B, T> addToBufferFn;
        private final DistributedConsumer<B> disposeBufferFn;

        private transient DistributedFunction<Processor.Context, B> newBufferFn;
        private transient DistributedConsumer<B> flushBufferFn;
        private transient HazelcastInstance client;

        HazelcastWriterSupplier(
                SerializableClientConfig clientConfig,
                DistributedFunction<HazelcastInstance, DistributedFunction<Processor.Context, B>> instanceToNewBufferFn,
                DistributedBiConsumer<B, T> addToBufferFn,
                DistributedFunction<HazelcastInstance, DistributedConsumer<B>> instanceToFlushBufferFn,
                DistributedConsumer<B> disposeBufferFn
        ) {
            this.clientConfig = clientConfig;
            this.instanceToFlushBufferFn = instanceToFlushBufferFn;
            this.instanceToNewBufferFn = instanceToNewBufferFn;
            this.addToBufferFn = addToBufferFn;
            this.disposeBufferFn = disposeBufferFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance;
            if (isRemote()) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
            flushBufferFn = instanceToFlushBufferFn.apply(instance);
            newBufferFn = instanceToNewBufferFn.apply(instance);
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        private boolean isRemote() {
            return clientConfig != null;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteBufferedP<>(newBufferFn, addToBufferFn, flushBufferFn, disposeBufferFn))
                         .limit(count).collect(toList());
        }
    }

    public static class ApplyFnEntryProcessor<K, V, T>
            implements EntryProcessor<K, V>, EntryBackupProcessor<K, V>, IdentifiedDataSerializable {
        private Map<K, Object> keysToUpdate;
        private DistributedBiFunction<? super V, ? super T, ? extends V> updateFn;

        public ApplyFnEntryProcessor() {
        }

        ApplyFnEntryProcessor(
                Map<K, Object> keysToUpdate,
                DistributedBiFunction<? super V, ? super T, ? extends V> updateFn
        ) {
            this.keysToUpdate = keysToUpdate;
            this.updateFn = updateFn;
        }

        @Override
        public Object process(Entry<K, V> entry) {
            Object item = keysToUpdate.get(entry.getKey());
            if (item == null && !keysToUpdate.containsKey(entry.getKey())) {
                // Implementing equals/hashCode is not required for IMap keys since serialized version is used
                // instead. After serializing/deserializing the keys they will have different identity. And since they
                // don't implement the methods, they key can't be found in the map.
                throw new JetException("The new item not found in the map - is equals/hashCode " +
                        "correctly implemented for the key? Key type: " + entry.getKey().getClass().getName());
            }
            if (item instanceof MultiItem) {
                for (T o : ((MultiItem<T>) item)) {
                    handle(entry, o);
                }
            } else {
                handle(entry, (T) item);
            }
            return null;
        }

        private void handle(Entry<K, V> entry, T item) {
            V oldValue = entry.getValue();
            V newValue = updateFn.apply(oldValue, item);
            entry.setValue(newValue);
        }

        @Override
        public EntryBackupProcessor<K, V> getBackupProcessor() {
            return this;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(keysToUpdate);
            out.writeObject(updateFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            keysToUpdate = in.readObject();
            updateFn = in.readObject();
        }

        @Override
        public void processBackup(Entry<K, V> entry) {
            process(entry);
        }

        @Override
        public int getFactoryId() {
            return SerializationConstants.FACTORY_ID;
        }

        @Override
        public int getId() {
            return SerializationConstants.APPLY_FN_ENTRY_PROCESSOR;
        }
    }
}
