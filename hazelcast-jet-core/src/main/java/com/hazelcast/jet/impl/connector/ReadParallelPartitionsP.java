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

import com.hazelcast.cache.impl.CacheEntryIterationResult;
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.operation.CacheEntryIteratorOperation;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.MigrationWatcher;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.IterationType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Private API, see methods in {@link SourceProcessors}.
 * <p>
 * The number of Hazelcast partitions should be configured to at least
 * {@code localParallelism * clusterSize}, otherwise some processors will
 * have no partitions assigned to them.
 */
public final class ReadParallelPartitionsP<R, E> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 16384;
    private final Reader<R, E> reader;
    private final int[] partitionIds;
    private final BooleanSupplier migrationWatcher;
    private final int[] readOffsets;

    private Future[] readFutures;

    // currently processed batch, it's partitionId and iterating position
    private List<E> batch = Collections.emptyList();
    private int currentPartitionIndex = -1;
    private int resultSetPosition;
    private int completedPartitions;

    private ReadParallelPartitionsP(
            @Nonnull Reader<R, E> reader,
            @Nonnull List<Integer> assignedPartitions,
            @Nonnull BooleanSupplier migrationWatcher
    ) {
        this.reader = reader;
        this.migrationWatcher = migrationWatcher;

        partitionIds = assignedPartitions.stream().mapToInt(Integer::intValue).toArray();
        readOffsets = new int[partitionIds.length];
        Arrays.fill(readOffsets, Integer.MAX_VALUE);

        assert partitionIds.length > 0 : "no partitions assigned";
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalCacheSupplier(@Nonnull String mapName) {
        return new LocalProcessorMetaSupplier<>(new LocalCacheReader(mapName));
    }

    @Nonnull
    public static ProcessorMetaSupplier readRemoteCacheSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        return new RemoteProcessorMetaSupplier<>(new RemoteCacheReader(mapName), clientConfig);
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalMapSupplier(@Nonnull String mapName) {
        return new LocalProcessorMetaSupplier<>(new LocalMapReader(mapName));
    }

    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readLocalMapSupplier(
            @Nonnull String mapName,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(Objects.requireNonNull(predicate), "predicate");
        checkSerializable(Objects.requireNonNull(projection), "projection");

        return new LocalProcessorMetaSupplier<>(new LocalMapQueryReader(mapName, predicate, projection));
    }

    @Nonnull
    public static ProcessorMetaSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        return new RemoteProcessorMetaSupplier<>(new RemoteMapReader(mapName), clientConfig);
    }

    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(Objects.requireNonNull(predicate), "predicate");
        checkSerializable(Objects.requireNonNull(projection), "projection");

        return new RemoteProcessorMetaSupplier<>(new RemoteMapQueryReader(mapName, predicate, projection), clientConfig);
    }

    private static <T> T translateFutureValue(Future future, Function<Object, T> transformation) {
        try {
            return transformation.apply(future.get());
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
        readFutures = new Future[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = reader.startBatch(partitionIds[i], readOffsets[i]);
        }
    }

    private boolean emitResultSet() {
        checkMigration();
        for (; resultSetPosition < batch.size(); resultSetPosition++) {
            Object result = reader.getFromBatch(batch, resultSetPosition);
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
        while (batch.size() == resultSetPosition && ++currentPartitionIndex < partitionIds.length) {

            if (readOffsets[currentPartitionIndex] < 0) { //partition is dompleted
                continue;
            }

            Future future = readFutures[currentPartitionIndex];
            if (!future.isDone()) { //data for partition not yet available
                continue;
            }

            R result = reader.getBatchResults(future);
            int nextTableIndexToReadFrom = reader.getNextTableIndexToReadFrom(result);

            if (nextTableIndexToReadFrom < 0) {
                completedPartitions++;
            } else {
                assert !batch.isEmpty() : "empty but not terminal batch";
            }

            batch = reader.getBatch(result);

            resultSetPosition = 0;
            readOffsets[currentPartitionIndex] = nextTableIndexToReadFrom;
            // make another read on the same partition
            readFutures[currentPartitionIndex] =
                    reader.startBatch(partitionIds[currentPartitionIndex], readOffsets[currentPartitionIndex]);
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            return false;
        }
        return true;
    }

    private static class LocalProcessorMetaSupplier<R, E> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private Reader<R, E> reader;
        private transient Map<Address, List<Integer>> addrToPartitions;

        LocalProcessorMetaSupplier(@Nonnull Reader<R, E> reader) {
            this.reader = reader;
        }

        @Override
        public void init(@Nonnull ProcessorMetaSupplier.Context context) {
            addrToPartitions = getPartitions(context).stream()
                    .collect(groupingBy(p -> p.getOwner().getAddress(),
                            mapping(Partition::getPartitionId, toList())));
        }

        private Set<Partition> getPartitions(Context context) {
            return context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions();
        }

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new LocalProcessorSupplier<>(reader, addrToPartitions.get(address));
        }
    }

    private static class LocalProcessorSupplier<R, E> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final Reader<R, E> reader;
        private final List<Integer> ownedPartitions;

        private transient BooleanSupplier migrationWatcher;

        LocalProcessorSupplier(
                @Nonnull Reader<R, E> reader,
                @Nonnull List<Integer> ownedPartitions
        ) {
            this.reader = reader;
            this.ownedPartitions = ownedPartitions;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstanceImpl hzInstance = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
            Node node = hzInstance.node;
            JetService jetService = node.nodeEngine.getService(JetService.SERVICE_NAME);
            reader.init(hzInstance);
            migrationWatcher = jetService.getSharedMigrationWatcher().createWatcher();
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
                return new ReadParallelPartitionsP<>(
                        reader,
                        partitions,
                        migrationWatcher
                );
            }
        }
    }

    private static class RemoteProcessorMetaSupplier<R, E> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final Reader<R, E> reader;

        private transient int remotePartitionCount;

        RemoteProcessorMetaSupplier(
                @Nonnull Reader<R, E> reader,
                @Nonnull ClientConfig clientConfig
        ) {
            this.reader = reader;
            this.clientXml = asXmlString(clientConfig);
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
            return address -> new RemoteProcessorSupplier<>(
                    clientXml,
                    reader,
                    roundRobinSubList(partitions, addresses.indexOf(address), addresses.size())
            );
        }
    }

    private static class RemoteProcessorSupplier<R, E> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final Reader<R, E> reader;
        private final List<Integer> ownedPartitions;

        private transient HazelcastInstance client;
        private transient MigrationWatcher migrationWatcher;

        RemoteProcessorSupplier(
                @Nonnull String clientXml,
                @Nonnull Reader<R, E> reader,
                @Nonnull List<Integer> ownedPartitions
        ) {
            this.clientXml = clientXml;
            this.reader = reader;
            this.ownedPartitions = ownedPartitions;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = newHazelcastClient(asClientConfig(clientXml));
            reader.init(client);
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
                return new ReadParallelPartitionsP<>(
                        reader,
                        partitions,
                        migrationWatcher.createWatcher()
                );
            }
        }
    }

    private abstract static class Reader<R, E> implements Serializable {

        protected transient InternalSerializationService serializationService;

        final String sourceName;
        final Predicate predicate;
        final Projection projection;

        Reader(
                @Nonnull String sourceName,
                @Nullable Predicate predicate,
                @Nullable Projection projection
        ) {
            this.sourceName = sourceName;
            this.predicate = predicate;
            this.projection = projection;
        }

        abstract void init(@Nonnull HazelcastInstance hzInstance);

        @Nullable
        abstract Future startBatch(int partitionId, int offset);

        @Nonnull
        abstract R getBatchResults(Future future);

        @Nonnull
        abstract List<E> getBatch(R result);

        abstract int getNextTableIndexToReadFrom(R result);

        @Nullable
        abstract Object getFromBatch(List<E> batch, int index);
    }

    private static class LocalCacheReader extends Reader<CacheEntryIterationResult, Entry<Data, Data>> {

        static final long serialVersionUID = 1L;

        private transient CacheProxy cacheProxy;

        LocalCacheReader(@Nonnull String cacheName) {
            super(cacheName, null, null);
        }

        @Override
        public void init(@Nonnull HazelcastInstance hzInstance) {
            this.cacheProxy = (CacheProxy) hzInstance.getCacheManager().getCache(sourceName);
            this.serializationService = (InternalSerializationService) cacheProxy.getNodeEngine()
                    .getSerializationService();
        }

        @Nullable
        @Override
        public Future startBatch(int partitionId, int offset) {
            if (offset < 0) {
                return null;
            }

            Operation op = new CacheEntryIteratorOperation(cacheProxy.getPrefixedName(), offset, MAX_FETCH_SIZE);
            //no access to CacheOperationProvider, have to be explicit

            OperationService operationService = cacheProxy.getOperationService();
            return operationService.invokeOnPartition(cacheProxy.getServiceName(), op, partitionId);
        }

        @Nonnull
        @Override
        public CacheEntryIterationResult getBatchResults(Future future) {
            return ((InternalCompletableFuture<CacheEntryIterationResult>) future).join();
        }

        @Nonnull
        @Override
        public List<Entry<Data, Data>> getBatch(CacheEntryIterationResult result) {
            return result.getEntries();
        }

        @Override
        public int getNextTableIndexToReadFrom(CacheEntryIterationResult result) {
            return result.getTableIndex();
        }

        @Nullable
        @Override
        public Object getFromBatch(List<Entry<Data, Data>> batch, int index) {
            Entry<Data, Data> dataEntry = batch.get(index);
            return new LazyMapEntry(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    private static class RemoteCacheReader extends Reader<CacheIterateEntriesCodec.ResponseParameters, Entry<Data, Data>> {

        static final long serialVersionUID = 1L;

        private transient ClientCacheProxy clientCacheProxy;

        RemoteCacheReader(@Nonnull String cacheName) {
            super(cacheName, null, null);
        }

        @Override
        public void init(@Nonnull HazelcastInstance hzInstance) {
            this.clientCacheProxy = (ClientCacheProxy) hzInstance.getCacheManager().getCache(sourceName);
            this.serializationService = clientCacheProxy.getContext().getSerializationService();
        }

        @Nullable
        @Override
        public Future startBatch(int partitionId, int offset) {
            String name = clientCacheProxy.getPrefixedName();
            ClientMessage request = CacheIterateEntriesCodec.encodeRequest(name, partitionId, offset, MAX_FETCH_SIZE);
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientCacheProxy.getContext()
                    .getHazelcastInstance();
            ClientInvocation clientInvocation = new ClientInvocation(client, request, name, partitionId);
            return clientInvocation.invoke();
        }

        @Nonnull
        @Override
        public CacheIterateEntriesCodec.ResponseParameters getBatchResults(Future future) {
            return translateFutureValue(future, o -> CacheIterateEntriesCodec.decodeResponse((ClientMessage) o));
        }

        @Nonnull
        @Override
        public List<Entry<Data, Data>> getBatch(CacheIterateEntriesCodec.ResponseParameters result) {
            return result.entries;
        }

        @Override
        public int getNextTableIndexToReadFrom(CacheIterateEntriesCodec.ResponseParameters result) {
            return result.tableIndex;
        }

        @Nullable
        @Override
        public Object getFromBatch(List<Entry<Data, Data>> batch, int index) {
            Entry<Data, Data> dataEntry = batch.get(index);
            return new LazyMapEntry(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    private static class LocalMapReader extends Reader<MapEntriesWithCursor, Entry<Data, Data>> {

        static final long serialVersionUID = 1L;

        private transient MapProxyImpl mapProxyImpl;

        LocalMapReader(@Nonnull String mapName) {
            super(mapName, null, null);
        }

        @Override
        public void init(@Nonnull HazelcastInstance hzInstance) {
            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(sourceName);
            this.serializationService = ((HazelcastInstanceImpl) hzInstance).getSerializationService();
        }

        @Nullable
        @Override
        public Future startBatch(int partitionId, int offset) {
            if (offset < 0) {
                return null;
            }

            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            Operation op = operationProvider.createFetchEntriesOperation(sourceName, offset, MAX_FETCH_SIZE);

            OperationService operationService = mapProxyImpl.getOperationService();
            return operationService.invokeOnPartition(mapProxyImpl.getServiceName(), op, partitionId);
        }

        @Nonnull
        @Override
        public MapEntriesWithCursor getBatchResults(Future future) {
            return translateFutureValue(future, o -> (MapEntriesWithCursor) o);
        }

        @Nonnull
        @Override
        public List<Entry<Data, Data>> getBatch(MapEntriesWithCursor result) {
            return result.getBatch();
        }

        @Override
        public int getNextTableIndexToReadFrom(MapEntriesWithCursor result) {
            return result.getNextTableIndexToReadFrom();
        }

        @Nullable
        @Override
        public Object getFromBatch(List<Entry<Data, Data>> batch, int index) {
            Entry<Data, Data> dataEntry = batch.get(index);
            return new LazyMapEntry(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    private static class LocalMapQueryReader extends Reader<ResultSegment, QueryResultRow> {

        static final long serialVersionUID = 1L;

        private transient MapProxyImpl mapProxyImpl;

        LocalMapQueryReader(
                @Nonnull String mapName,
                @Nonnull Predicate predicate,
                @Nonnull Projection projection
        ) {
            super(mapName, predicate, projection);
        }

        @Override
        public void init(@Nonnull HazelcastInstance hzInstance) {
            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(sourceName);
            this.serializationService = ((HazelcastInstanceImpl) hzInstance).getSerializationService();
        }

        @Nullable
        @Override
        public Future startBatch(int partitionId, int offset) {
            if (offset < 0) {
                return null;
            }

            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            MapOperation op = operationProvider.createFetchWithQueryOperation(
                    sourceName,
                    offset,
                    MAX_FETCH_SIZE,
                    Query.of()
                            .mapName(sourceName)
                            .iterationType(IterationType.VALUE)
                            .predicate(predicate)
                            .projection(projection)
                            .build()
            );

            OperationService operationService = mapProxyImpl.getOperationService();
            return operationService.invokeOnPartition(mapProxyImpl.getServiceName(), op, partitionId);
        }

        @Nonnull
        @Override
        public ResultSegment getBatchResults(Future future) {
            return translateFutureValue(future, o -> (ResultSegment) o);
        }

        @Nonnull
        @Override
        public List<QueryResultRow> getBatch(ResultSegment result) {
            QueryResult queryResult = (QueryResult) result.getResult();
            return queryResult.getRows();
        }

        @Override
        public int getNextTableIndexToReadFrom(ResultSegment result) {
            return result.getNextTableIndexToReadFrom();
        }

        @Nullable
        @Override
        public Object getFromBatch(List<QueryResultRow> batch, int index) {
            return serializationService.toObject(batch.get(index).getValue());
        }
    }

    private static class RemoteMapReader extends Reader<MapFetchEntriesCodec.ResponseParameters, Entry<Data, Data>> {

        static final long serialVersionUID = 1L;

        private transient ClientMapProxy clientMapProxy;

        RemoteMapReader(@Nonnull String mapName) {
            super(mapName, null, null);
        }

        @Override
        public void init(@Nonnull HazelcastInstance hzInstance) {
            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(sourceName);
            this.serializationService = clientMapProxy.getContext().getSerializationService();
        }

        @Override
        @Nullable
        public Future startBatch(int partitionId, int offset) {
            if (offset < 0) {
                return null;
            }
            ClientMessage request = MapFetchEntriesCodec.encodeRequest(sourceName, partitionId, offset, MAX_FETCH_SIZE);
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    sourceName,
                    partitionId
            );
            return clientInvocation.invoke();
        }

        @Nonnull
        @Override
        public MapFetchEntriesCodec.ResponseParameters getBatchResults(Future future) {
            return translateFutureValue(future, o -> MapFetchEntriesCodec.decodeResponse((ClientMessage) o));
        }

        @Nonnull
        @Override
        public List<Entry<Data, Data>> getBatch(MapFetchEntriesCodec.ResponseParameters result) {
            return result.entries;
        }

        @Override
        public int getNextTableIndexToReadFrom(MapFetchEntriesCodec.ResponseParameters result) {
            return result.tableIndex;
        }

        @Nullable
        @Override
        public Object getFromBatch(List<Entry<Data, Data>> batch, int index) {
            Entry<Data, Data> dataEntry = batch.get(index);
            return new LazyMapEntry(dataEntry.getKey(), dataEntry.getValue(), serializationService);
        }
    }

    private static class RemoteMapQueryReader extends Reader<MapFetchWithQueryCodec.ResponseParameters, Data> {

        static final long serialVersionUID = 1L;

        private transient ClientMapProxy clientMapProxy;

        RemoteMapQueryReader(
                @Nonnull String mapName,
                @Nonnull Predicate predicate,
                @Nonnull Projection projection
        ) {
            super(mapName, predicate, projection);
        }

        @Override
        public void init(@Nonnull HazelcastInstance hzInstance) {
            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(sourceName);
            this.serializationService = clientMapProxy.getContext().getSerializationService();
        }

        @Override
        @Nullable
        public Future startBatch(int partitionId, int offset) {
            if (offset < 0) {
                return null;
            }
            ClientMessage request = MapFetchWithQueryCodec.encodeRequest(sourceName, offset, MAX_FETCH_SIZE,
                    serializationService.toData(projection),
                    serializationService.toData(predicate));
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    sourceName,
                    partitionId
            );
            return clientInvocation.invoke();
        }

        @Nonnull
        @Override
        public MapFetchWithQueryCodec.ResponseParameters getBatchResults(Future future) {
            return translateFutureValue(future, o -> MapFetchWithQueryCodec.decodeResponse((ClientMessage) o));
        }

        @Nonnull
        @Override
        public List<Data> getBatch(MapFetchWithQueryCodec.ResponseParameters result) {
            return result.results;
        }

        @Override
        public int getNextTableIndexToReadFrom(MapFetchWithQueryCodec.ResponseParameters result) {
            return result.nextTableIndexToReadFrom;
        }

        @Nullable
        @Override
        public Object getFromBatch(List<Data> batch, int index) {
            return serializationService.toObject(batch.get(index));
        }
    }
}
