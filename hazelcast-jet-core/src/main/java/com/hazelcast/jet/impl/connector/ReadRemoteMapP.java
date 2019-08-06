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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.MigrationWatcher;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.asClientConfig;
import static com.hazelcast.jet.impl.util.Util.asXmlString;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static java.util.stream.Collectors.toList;

public final class ReadRemoteMapP<K, V, T> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 16384;

    private final Predicate<? super K, ? super V> predicate;
    private final Projection<? super Map.Entry<K, V>, ? extends T> projection;
    private final int[] partitionIds;
    private final BooleanSupplier migrationWatcher;
    private final int[] readOffsets;
    private final InternalSerializationService serializationService;
    private final ClientMapProxy mapProxy;

    private ClientInvocationFuture[] readFutures;

    // currently processed batch, it's partitionId and iterating position
    private List batch = Collections.emptyList();
    private int currentPartitionIndex = -1;
    private int resultSetPosition;
    private int completedPartitions;

    private ReadRemoteMapP(
            @Nonnull HazelcastInstance client,
            @Nonnull String mapName,
            @Nonnull List<Integer> assignedPartitions,
            @Nonnull BooleanSupplier migrationWatcher,
            @Nullable Predicate<? super K, ? super V> predicate,
            @Nullable Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        this.mapProxy = (ClientMapProxy) ((HazelcastClientProxy) client).client.getMap(mapName);
        this.serializationService = mapProxy.getContext().getSerializationService();
        this.predicate = predicate;
        this.projection = projection;
        this.migrationWatcher = migrationWatcher;

        partitionIds = assignedPartitions.stream().mapToInt(Integer::intValue).toArray();
        readOffsets = new int[partitionIds.length];
        Arrays.fill(readOffsets, Integer.MAX_VALUE);

        assert partitionIds.length > 0 : "no partitions assigned";
    }

    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nullable Predicate<? super K, ? super V> predicate,
            @Nullable Projection<? super Map.Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new ClusterMetaSupplier<>(mapName, clientConfig, predicate, projection);
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        while (emitResultSet()) {
            if (!tryGetNextResultSet()) {
                return completedPartitions == partitionIds.length;
            }
        }
        return false;
    }

    private void initialRead() {
        readFutures = new ClientInvocationFuture[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = readFromMap(partitionIds[i], readOffsets[i]);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean emitResultSet() {
        checkMigration();
        boolean queryNeeded = queryNeeded();
        for (; resultSetPosition < batch.size(); resultSetPosition++) {
            Object data = batch.get(resultSetPosition);
            Object result;
            if (queryNeeded) {
                result = serializationService.toObject(data);
            } else {
                Entry<Data, Data> dataEntry = (Entry<Data, Data>) data;
                result = new LazyMapEntry(dataEntry.getKey(), dataEntry.getValue(), serializationService);
            }
            if (result == null) {
                continue;
            }
            if (!tryEmit(result)) {
                return false;
            }
        }
        // we're done with current batch
        return true;
    }

    private void checkMigration() {
        if (migrationWatcher.getAsBoolean()) {
            throw new RestartableException("Partition migration detected");
        }
    }

    private boolean tryGetNextResultSet() {
        boolean queryNeeded = queryNeeded();
        while (batch.size() == resultSetPosition && ++currentPartitionIndex < partitionIds.length) {
            ClientInvocationFuture future = readFutures[currentPartitionIndex];
            if (future == null || !future.isDone()) {
                continue;
            }
            Object result = toResultSet(future);
            int nextTableIndexToReadFrom = getNextTableIndexToReadFrom(result, queryNeeded);

            if (nextTableIndexToReadFrom < 0) {
                completedPartitions++;
            } else {
                assert !batch.isEmpty() : "empty but not terminal batch";
            }

            batch = getBatch(result, queryNeeded);

            resultSetPosition = 0;
            readOffsets[currentPartitionIndex] = nextTableIndexToReadFrom;
            // make another read on the same partition
            readFutures[currentPartitionIndex] =
                    readFromMap(partitionIds[currentPartitionIndex], readOffsets[currentPartitionIndex]);
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            return false;
        }
        return true;
    }

    private List getBatch(Object result, boolean queryNeeded) {
        if (queryNeeded) {
            return ((MapFetchWithQueryCodec.ResponseParameters) result).results;
        } else {
            return ((MapFetchEntriesCodec.ResponseParameters) result).entries;
        }
    }

    private int getNextTableIndexToReadFrom(Object result, boolean queryNeeded) {
        if (queryNeeded) {
            return ((MapFetchWithQueryCodec.ResponseParameters) result).nextTableIndexToReadFrom;
        } else {
            return ((MapFetchEntriesCodec.ResponseParameters) result).tableIndex;
        }
    }

    private Object toResultSet(ClientInvocationFuture future) {
        try {
            if (queryNeeded()) {
                return MapFetchWithQueryCodec.decodeResponse(future.get());
            } else {
                return MapFetchEntriesCodec.decodeResponse(future.get());
            }
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastSerializationException) {
                throw new JetException("Serialization error when reading the map: are the key, value, " +
                        "predicate and projection classes visible to IMDG? You need to use User Code " +
                        "Deployment, adding the classes to JetConfig isn't enough", e);
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private ClientInvocationFuture readFromMap(int partitionId, int offset) {
        if (offset < 0) {
            return null;
        }
        ClientMessage request = queryNeeded() ?
                MapFetchWithQueryCodec.encodeRequest(mapProxy.getName(), offset, MAX_FETCH_SIZE,
                        serializationService.toData(projection),
                        serializationService.toData(predicate)) :
                MapFetchEntriesCodec.encodeRequest(mapProxy.getName(), partitionId, offset, MAX_FETCH_SIZE);
        ClientInvocation clientInvocation = new ClientInvocation(
                (HazelcastClientInstanceImpl) mapProxy.getContext().getHazelcastInstance(),
                request,
                mapProxy.getName(),
                partitionId
        );
        return clientInvocation.invoke();
    }

    private boolean queryNeeded() {
        return predicate != null || projection != null;
    }

    private static class ClusterMetaSupplier<K, V, T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final String mapName;
        private final Predicate<? super K, ? super V> predicate;
        private final Projection<? super Map.Entry<K, V>, ? extends T> projection;

        private transient int remotePartitionCount;

        ClusterMetaSupplier(
                @Nonnull String mapName,
                @Nonnull ClientConfig clientConfig,
                @Nullable Predicate<? super K, ? super V> predicate,
                @Nullable Projection<? super Map.Entry<K, V>, ? extends T> projection
        ) {
            this.mapName = mapName;
            this.clientXml = asXmlString(clientConfig);
            this.predicate = predicate;
            this.projection = projection;
        }

        /**
         * Assigns the {@code list} to {@code count} sublists in a round-robin fashion. One call returns
         * the {@code index}-th sublist.
         *
         * <p>For example, for a 7-element list where {@code count == 3}, it would respectively return for
         * indices 0..2:
         *
         * <pre>
         *   0, 3, 6
         *   1, 4
         *   2, 5
         * </pre>
         */
        @Nonnull
        private static <T> List<T> roundRobinSubList(@Nonnull List<T> list, int index, int count) {
            if (index < 0 || index >= count) {
                throw new IllegalArgumentException("index=" + index + ", count=" + count);
            }
            return IntStream.range(0, list.size())
                    .filter(i -> i % count == index)
                    .mapToObj(list::get)
                    .collect(toList());
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public void init(@Nonnull ProcessorMetaSupplier.Context context) {
            remotePartitionCount = getPartitionCount();

        }

        private int getPartitionCount() {
            HazelcastInstance client = newHazelcastClient(asClientConfig(clientXml));
            try {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
                ClientPartitionService partitionService = clientProxy.client.getClientPartitionService();
                return partitionService.getPartitionCount();
            } finally {
                client.shutdown();
            }
        }

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            List<Integer> partitions = IntStream.rangeClosed(0, remotePartitionCount - 1).boxed().collect(toList());
            return address -> new ClusterProcessorSupplier<>(
                    clientXml,
                    mapName,
                    roundRobinSubList(partitions, addresses.indexOf(address), addresses.size()),
                    predicate, projection);
        }
    }

    private static class ClusterProcessorSupplier<K, V, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final String mapName;
        private final List<Integer> ownedPartitions;
        private final Predicate<? super K, ? super V> predicate;
        private final Projection<? super Map.Entry<K, V>, ? extends T> projection;

        private transient HazelcastInstance client;
        private transient MigrationWatcher migrationWatcher;

        ClusterProcessorSupplier(
                @Nonnull String clientXml,
                @Nonnull String mapName,
                @Nonnull List<Integer> ownedPartitions,
                @Nullable Predicate<? super K, ? super V> predicate,
                @Nullable Projection<? super Map.Entry<K, V>, ? extends T> projection
        ) {
            this.clientXml = clientXml;
            this.mapName = mapName;
            this.ownedPartitions = ownedPartitions;
            this.predicate = predicate;
            this.projection = projection;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = newHazelcastClient(asClientConfig(clientXml));
            migrationWatcher = new MigrationWatcher(client);
        }

        @Override
        public void close(Throwable error) {
            if (migrationWatcher != null) {
                migrationWatcher.deregister();
            }
            if (client != null) {
                client.shutdown();
            }
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            return processorToPartitions(count, ownedPartitions)
                    .values().stream()
                    .map(this::processorForPartitions)
                    .collect(toList());
        }

        private Processor processorForPartitions(List<Integer> partitions) {
            if (partitions.isEmpty()) {
                return Processors.noopP().get();
            } else {
                return new ReadRemoteMapP<>(
                        client,
                        mapName,
                        partitions,
                        migrationWatcher.createWatcher(),
                        predicate, projection
                );
            }
        }
    }

}
