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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedIntFunction;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
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
    public static ProcessorMetaSupplier writeMapP(String name, ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return dontParallelize(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayMap(),
                ArrayMap::add,
                instance -> {
                    IMap map = instance.getMap(name);
                    return buffer -> {
                        try {
                            map.putAll(buffer);
                        } catch (HazelcastInstanceNotActiveException e) {
                            handleInstanceNotActive(instance, e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                noopConsumer()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeCacheP(String name, ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return dontParallelize(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayMap(),
                ArrayMap::add,
                CacheFlush.flushToCache(name, isLocal),
                noopConsumer()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListP(String name, ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return dontParallelize(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayList<>(),
                ArrayList::add,
                instance -> {
                    IList<Object> list = instance.getList(name);
                    return buffer -> {
                        try {
                            list.addAll(buffer);
                        } catch (HazelcastInstanceNotActiveException e) {
                            handleInstanceNotActive(instance, e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                noopConsumer()
        ));
    }

    private static void handleInstanceNotActive(
            HazelcastInstance instance, HazelcastInstanceNotActiveException e, boolean isLocal
    ) {
        if (isLocal) {
            // if we are writing to a local instance, we can safely ignore this exception
            // as the job will eventually restart on its own.
            instance.getLoggingService().getLogger(HazelcastWriters.class).fine(
                    "Ignoring HazelcastInstanceNotActiveException from local cluster as the job will be" +
                            " restarted automatically.", e);
            return;
        }
        throw e;
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
                        handleInstanceNotActive(instance, e, isLocal);
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

    private static class HazelcastWriterSupplier<B, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig clientConfig;
        private final DistributedFunction<HazelcastInstance, DistributedConsumer<B>> instanceToFlushBuffer;
        private final DistributedIntFunction<B> bufferSupplier;
        private final DistributedBiConsumer<B, T> addToBuffer;
        private final DistributedConsumer<B> disposeBuffer;

        private transient DistributedConsumer<B> flushBuffer;
        private transient HazelcastInstance client;

        HazelcastWriterSupplier(
                SerializableClientConfig clientConfig,
                DistributedIntFunction<B> newBuffer,
                DistributedBiConsumer<B, T> addToBuffer,
                DistributedFunction<HazelcastInstance, DistributedConsumer<B>> instanceToFlushBuffer,
                DistributedConsumer<B> disposeBuffer
        ) {
            this.clientConfig = clientConfig;
            this.instanceToFlushBuffer = instanceToFlushBuffer;
            this.bufferSupplier = newBuffer;
            this.addToBuffer = addToBuffer;
            this.disposeBuffer = disposeBuffer;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance;
            if (isRemote()) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
            flushBuffer = instanceToFlushBuffer.apply(instance);
        }

        @Override
        public void complete(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        private boolean isRemote() {
            return clientConfig != null;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteBufferedP<>(bufferSupplier, addToBuffer, flushBuffer, disposeBuffer))
                         .limit(count).collect(toList());
        }
    }
}
