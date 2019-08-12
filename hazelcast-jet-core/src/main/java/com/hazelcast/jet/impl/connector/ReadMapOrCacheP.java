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
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec.ResponseParameters;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.MigrationWatcher;
import com.hazelcast.jet.impl.util.Util;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.asClientConfig;
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
 *
 * @param <R> reader's result type
 * @param <E> reader's result element type
 */
public final class ReadMapOrCacheP<R, E> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 16384;

    private final Reader<R, E> reader;
    private final int[] partitionIds;
    private final BooleanSupplier migrationWatcher;
    private final int[] readOffsets;

    private Future[] readFutures;

    // currently emitted batch, its iterating position and partitionId
    private List<E> currentBatch = Collections.emptyList();
    private int currentBatchPosition;
    private int currentPartitionIndex = -1;
    private int numCompletedPartitions;

    private ReadMapOrCacheP(
            @Nonnull Reader<R, E> reader,
            @Nonnull int[] partitionIds,
            @Nonnull BooleanSupplier migrationWatcher
    ) {
        this.reader = reader;
        this.partitionIds = partitionIds;
        this.migrationWatcher = migrationWatcher;

        readOffsets = new int[partitionIds.length];
        Arrays.fill(readOffsets, Integer.MAX_VALUE);
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        while (emitResultSet()) {
            if (!tryGetNextResultSet()) {
                return numCompletedPartitions == partitionIds.length;
            }
        }
        return false;
    }

    private void initialRead() {
        readFutures = new Future[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = reader.readBatch(partitionIds[i], Integer.MAX_VALUE);
        }
    }

    private boolean emitResultSet() {
        checkMigration();
        for (; currentBatchPosition < currentBatch.size(); currentBatchPosition++) {
            Object result = reader.getFromBatch(currentBatch, currentBatchPosition);
            if (result == null) {
                // element was filtered out by the predicate (?)
                continue;
            }
            if (!tryEmit(result)) {
                return false;
            }
        }
        // we're done with the current batch
        return true;
    }

    private boolean tryGetNextResultSet() {
        while (currentBatch.size() == currentBatchPosition && ++currentPartitionIndex < partitionIds.length) {
            if (readOffsets[currentPartitionIndex] < 0) {  // partition is completed
                assert readFutures[currentPartitionIndex] == null : "future not null";
                continue;
            }

            Future future = readFutures[currentPartitionIndex];
            if (!future.isDone()) {  // data for partition not yet available
                continue;
            }

            R result = reader.getBatchResults(future);
            int nextTableIndexToReadFrom = reader.getNextTableIndexToReadFrom(result);

            if (nextTableIndexToReadFrom < 0) {
                numCompletedPartitions++;
            } else {
                assert !currentBatch.isEmpty() : "empty but not terminal batch";
            }

            currentBatch = reader.getBatch(result);

            currentBatchPosition = 0;
            readOffsets[currentPartitionIndex] = nextTableIndexToReadFrom;
            // make another read on the same partition
            readFutures[currentPartitionIndex] = readOffsets[currentPartitionIndex] >= 0
                    ? reader.readBatch(partitionIds[currentPartitionIndex], readOffsets[currentPartitionIndex])
                    : null;
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            return false;
        }
        return true;
    }

    private void checkMigration() {
        if (migrationWatcher.getAsBoolean()) {
            throw new RestartableException("Partition migration detected");
        }
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalCacheSupplier(@Nonnull String cacheName) {
        return new LocalProcessorMetaSupplier<>(hzInstance -> new LocalCacheReader(hzInstance, cacheName));
    }

    @Nonnull
    public static ProcessorSupplier readRemoteCacheSupplier(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        String clientXml = Util.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, hzInstance -> new RemoteCacheReader(hzInstance, cacheName));
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalMapSupplier(@Nonnull String mapName) {
        return new LocalProcessorMetaSupplier<>(hzInstance -> new LocalMapReader(hzInstance, mapName));
    }

    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readLocalMapSupplier(
            @Nonnull String mapName,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(Objects.requireNonNull(predicate), "predicate");
        checkSerializable(Objects.requireNonNull(projection), "projection");

        return new LocalProcessorMetaSupplier<>(
                hzInstance -> new LocalMapQueryReader(hzInstance, mapName, predicate, projection));
    }

    @Nonnull
    public static ProcessorSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        String clientXml = Util.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, hzInstance -> new RemoteMapReader(hzInstance, mapName));
    }

    @Nonnull
    public static <K, V, T> ProcessorSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(Objects.requireNonNull(predicate), "predicate");
        checkSerializable(Objects.requireNonNull(projection), "projection");

        String clientXml = Util.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml,
                hzInstance -> new RemoteMapQueryReader(hzInstance, mapName, predicate, projection));
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

    private static class LocalProcessorMetaSupplier<R, E> implements ProcessorMetaSupplier {

        private static final long serialVersionUID = 1L;
        private final FunctionEx<HazelcastInstance, Reader<R, E>> readerSupplier;
        private transient Map<Address, List<Integer>> addrToPartitions;

        LocalProcessorMetaSupplier(@Nonnull FunctionEx<HazelcastInstance, Reader<R, E>> readerSupplier) {
            this.readerSupplier = readerSupplier;
        }

        @Override
        public void init(@Nonnull ProcessorMetaSupplier.Context context) {
            Set<Partition> partitions = context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions();
            addrToPartitions = partitions.stream()
                    .collect(groupingBy(
                            p -> p.getOwner().getAddress(),
                            mapping(Partition::getPartitionId, toList())));
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new LocalProcessorSupplier<>(readerSupplier, addrToPartitions.get(address));
        }
    }

    private static final class LocalProcessorSupplier<R, E> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final Function<HazelcastInstance, Reader<R, E>> readerSupplier;
        private final List<Integer> memberPartitions;

        private transient BooleanSupplier migrationWatcher;
        private transient HazelcastInstanceImpl hzInstance;

        private LocalProcessorSupplier(Function<HazelcastInstance, Reader<R, E>> readerSupplier,
                                       List<Integer> memberPartitions) {
            this.readerSupplier = readerSupplier;
            this.memberPartitions = memberPartitions;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            hzInstance = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
            JetService jetService = hzInstance.node.nodeEngine.getService(JetService.SERVICE_NAME);
            migrationWatcher = jetService.getSharedMigrationWatcher().createWatcher();
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return processorToPartitions(count, memberPartitions).values().stream()
                    .map(partitions -> partitions.stream().mapToInt(Integer::intValue).toArray())
                    .map(partitions -> new ReadMapOrCacheP<>(readerSupplier.apply(hzInstance), partitions,
                            migrationWatcher))
                    .collect(toList());
        }
    }

    private static class RemoteProcessorSupplier<R, E> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final FunctionEx<HazelcastInstance, Reader<R, E>> readerSupplier;

        private transient HazelcastClientProxy client;
        private transient MigrationWatcher migrationWatcher;
        private transient int totalParallelism;
        private transient int baseIndex;

        RemoteProcessorSupplier(
                @Nonnull String clientXml,
                FunctionEx<HazelcastInstance, Reader<R, E>> readerSupplier
        ) {
            this.clientXml = clientXml;
            this.readerSupplier = readerSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = (HazelcastClientProxy) newHazelcastClient(asClientConfig(clientXml));
            migrationWatcher = new MigrationWatcher(client);
            totalParallelism = context.totalParallelism();
            baseIndex = context.memberIndex() * context.localParallelism();
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

        @Override @Nonnull
        public List<Processor> get(int count) {
            int remotePartitionCount = client.client.getClientPartitionService().getPartitionCount();
            BooleanSupplier watcherInstance = migrationWatcher.createWatcher();

            return IntStream.range(0, count)
                     .mapToObj(i -> {
                         int[] partitionIds = Util.roundRobinPart(remotePartitionCount, totalParallelism, baseIndex + i);
                         return new ReadMapOrCacheP<>(readerSupplier.apply(client), partitionIds, watcherInstance);
                     })
                     .collect(Collectors.toList());
        }
    }

    /**
     * Stateless interface to read a map/cache.
     *
     * @param <R> result type
     * @param <E> result element type
     */
    private abstract static class Reader<R, E> {

        protected InternalSerializationService serializationService;

        final String objectName;
        final Predicate predicate;
        final Projection projection;

        Reader(
                @Nonnull String objectName,
                @Nullable Predicate predicate,
                @Nullable Projection projection
        ) {
            this.objectName = objectName;
            this.predicate = predicate;
            this.projection = projection;
        }

        @Nonnull
        abstract Future readBatch(int partitionId, int offset);

        @Nonnull
        abstract R getBatchResults(Future future);

        @Nonnull
        abstract List<E> getBatch(R result);

        abstract int getNextTableIndexToReadFrom(R result);

        @Nullable
        abstract Object getFromBatch(List<E> batch, int index);
    }

    private static class LocalCacheReader extends Reader<CacheEntryIterationResult, Entry<Data, Data>> {

        private transient CacheProxy cacheProxy;

        LocalCacheReader(HazelcastInstance hzInstance, @Nonnull String cacheName) {
            super(cacheName, null, null);

            this.cacheProxy = (CacheProxy) hzInstance.getCacheManager().getCache(cacheName);
            this.serializationService = (InternalSerializationService) cacheProxy.getNodeEngine()
                                                                                 .getSerializationService();
        }

        @Nonnull @Override
        public Future readBatch(int partitionId, int offset) {
            assert offset >= 0 : "offset=" + offset;

            Operation op = new CacheEntryIteratorOperation(cacheProxy.getPrefixedName(), offset, MAX_FETCH_SIZE);
            //no access to CacheOperationProvider, have to be explicit

            OperationService operationService = cacheProxy.getOperationService();
            return operationService.invokeOnPartition(cacheProxy.getServiceName(), op, partitionId);
        }

        @Nonnull @Override
        public CacheEntryIterationResult getBatchResults(Future future) {
            return ((InternalCompletableFuture<CacheEntryIterationResult>) future).join();
        }

        @Nonnull @Override
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

        private static final Function<Object, CacheIterateEntriesCodec.ResponseParameters> TRANSLATE_RESULT_FN =
                o -> CacheIterateEntriesCodec.decodeResponse((ClientMessage) o);

        private transient ClientCacheProxy clientCacheProxy;

        RemoteCacheReader(HazelcastInstance hzInstance, @Nonnull String cacheName) {
            super(cacheName, null, null);
            this.clientCacheProxy = (ClientCacheProxy) hzInstance.getCacheManager().getCache(cacheName);
            this.serializationService = clientCacheProxy.getContext().getSerializationService();
        }

        @Nonnull @Override
        public Future readBatch(int partitionId, int offset) {
            assert offset >= 0 : "offset=" + offset;
            String name = clientCacheProxy.getPrefixedName();
            ClientMessage request = CacheIterateEntriesCodec.encodeRequest(name, partitionId, offset, MAX_FETCH_SIZE);
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientCacheProxy.getContext()
                    .getHazelcastInstance();
            ClientInvocation clientInvocation = new ClientInvocation(client, request, name, partitionId);
            return clientInvocation.invoke();
        }

        @Nonnull @Override
        public CacheIterateEntriesCodec.ResponseParameters getBatchResults(Future future) {
            return translateFutureValue(future, TRANSLATE_RESULT_FN);
        }

        @Nonnull @Override
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

        private static final Function<Object, MapEntriesWithCursor> TRANSLATE_RESULT_FN = o -> (MapEntriesWithCursor) o;

        private MapProxyImpl mapProxyImpl;

        LocalMapReader(@Nonnull HazelcastInstance hzInstance, @Nonnull String mapName) {
            super(mapName, null, null);

            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(mapName);
            this.serializationService = ((HazelcastInstanceImpl) hzInstance).getSerializationService();
        }

        @Nonnull @Override
        public Future readBatch(int partitionId, int offset) {
            assert offset >= 0 : "offset=" + offset;

            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            Operation op = operationProvider.createFetchEntriesOperation(objectName, offset, MAX_FETCH_SIZE);

            OperationService operationService = mapProxyImpl.getOperationService();
            return operationService.invokeOnPartition(mapProxyImpl.getServiceName(), op, partitionId);
        }

        @Nonnull @Override
        public MapEntriesWithCursor getBatchResults(Future future) {
            return translateFutureValue(future, TRANSLATE_RESULT_FN);
        }

        @Nonnull @Override
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
        private static final Function<Object, ResultSegment> TRANSLATE_RESULT_FN = o -> (ResultSegment) o;

        private transient MapProxyImpl mapProxyImpl;

        LocalMapQueryReader(
                @Nonnull HazelcastInstance hzInstance,
                @Nonnull String mapName,
                @Nonnull Predicate predicate,
                @Nonnull Projection projection
        ) {
            super(mapName, predicate, projection);

            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(mapName);
            this.serializationService = ((HazelcastInstanceImpl) hzInstance).getSerializationService();
        }

        @Nonnull @Override
        public Future readBatch(int partitionId, int offset) {
            assert offset >= 0 : "offset=" + offset;

            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            MapOperation op = operationProvider.createFetchWithQueryOperation(
                    objectName,
                    offset,
                    MAX_FETCH_SIZE,
                    Query.of()
                            .mapName(objectName)
                            .iterationType(IterationType.VALUE)
                            .predicate(predicate)
                            .projection(projection)
                            .build()
            );

            OperationService operationService = mapProxyImpl.getOperationService();
            return operationService.invokeOnPartition(mapProxyImpl.getServiceName(), op, partitionId);
        }

        @Nonnull @Override
        public ResultSegment getBatchResults(Future future) {
            return translateFutureValue(future, TRANSLATE_RESULT_FN);
        }

        @Nonnull @Override
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

        private static final Function<Object, ResponseParameters> TRANSLATE_RESULT_FN =
                o -> MapFetchEntriesCodec.decodeResponse((ClientMessage) o);

        private transient ClientMapProxy clientMapProxy;

        RemoteMapReader(@Nonnull HazelcastInstance hzInstance, @Nonnull String mapName) {
            super(mapName, null, null);

            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(mapName);
            this.serializationService = clientMapProxy.getContext().getSerializationService();
        }

        @Nonnull @Override
        public Future readBatch(int partitionId, int offset) {
            assert offset >= 0 : "offset=" + offset;
            ClientMessage request = MapFetchEntriesCodec.encodeRequest(objectName, partitionId, offset, MAX_FETCH_SIZE);
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    objectName,
                    partitionId
            );
            return clientInvocation.invoke();
        }

        @Nonnull @Override
        public MapFetchEntriesCodec.ResponseParameters getBatchResults(Future future) {
            return translateFutureValue(future, TRANSLATE_RESULT_FN);
        }

        @Nonnull @Override
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
        private static final Function<Object, MapFetchWithQueryCodec.ResponseParameters> TRANSLATE_RESULT_FN =
                o -> MapFetchWithQueryCodec.decodeResponse((ClientMessage) o);

        private transient ClientMapProxy clientMapProxy;

        RemoteMapQueryReader(
                @Nonnull HazelcastInstance hzInstance,
                @Nonnull String mapName,
                @Nonnull Predicate predicate,
                @Nonnull Projection projection
        ) {
            super(mapName, predicate, projection);

            this.clientMapProxy = (ClientMapProxy) hzInstance.getMap(mapName);
            this.serializationService = clientMapProxy.getContext().getSerializationService();
        }

        @Nonnull @Override
        public Future readBatch(int partitionId, int offset) {
            assert offset >= 0 : "offset=" + offset;

            ClientMessage request = MapFetchWithQueryCodec.encodeRequest(objectName, offset, MAX_FETCH_SIZE,
                    serializationService.toData(projection),
                    serializationService.toData(predicate));
            ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientMapProxy.getContext().getHazelcastInstance(),
                    request,
                    objectName,
                    partitionId
            );
            return clientInvocation.invoke();
        }

        @Nonnull @Override
        public MapFetchWithQueryCodec.ResponseParameters getBatchResults(Future future) {
            return translateFutureValue(future, TRANSLATE_RESULT_FN);
        }

        @Nonnull @Override
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
