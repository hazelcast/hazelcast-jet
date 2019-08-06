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

import com.hazelcast.core.ICompletableFuture;
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
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.operation.MapFetchEntriesOperation;
import com.hazelcast.map.impl.operation.MapFetchWithQueryOperation;
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
import com.hazelcast.spi.Operation;
import com.hazelcast.util.IterationType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public final class ReadMapP<K, V, T> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 16384;

    private final String mapName;
    private final Predicate<? super K, ? super V> predicate;
    private final Projection<? super Entry<K, V>, ? extends T> projection;
    private final int[] partitionIds;
    private final BooleanSupplier migrationWatcher;
    private final int[] readOffsets;

    private MapProxyImpl mapProxy;
    private ICompletableFuture<ResultSegment>[] readFutures;

    // currently processed batch, it's partitionId and iterating position
    private List batch = Collections.emptyList();
    private int currentPartitionIndex = -1;
    private int resultSetPosition;
    private int completedPartitions;
    private InternalSerializationService serializationService;

    private ReadMapP(
            @Nonnull String mapName,
            @Nonnull List<Integer> assignedPartitions,
            @Nonnull BooleanSupplier migrationWatcher,
            @Nullable Predicate<? super K, ? super V> predicate,
            @Nullable Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        this.mapName = mapName;
        this.predicate = predicate;
        this.projection = projection;
        this.migrationWatcher = migrationWatcher;

        partitionIds = assignedPartitions.stream().mapToInt(Integer::intValue).toArray();
        readOffsets = new int[partitionIds.length];
        Arrays.fill(readOffsets, Integer.MAX_VALUE);

        assert partitionIds.length > 0 : "no partitions assigned";
    }

    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMapSupplier(
            @Nonnull String mapName,
            @Nullable Predicate<? super K, ? super V> predicate,
            @Nullable Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new ClusterMetaSupplier<>(mapName, predicate, projection);
    }

    @Override
    protected void init(Context context) {
        HazelcastInstanceImpl hzInstance = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
        mapProxy = (MapProxyImpl) hzInstance.getMap(mapName);
        serializationService = hzInstance.getSerializationService();
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

    @SuppressWarnings("unchecked")
    private void initialRead() {
        readFutures = new ICompletableFuture[partitionIds.length];
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
                result = serializationService.toObject(((QueryResultRow) data).getValue());
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
            ICompletableFuture future = readFutures[currentPartitionIndex];
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
            QueryResult queryResult = (QueryResult) ((ResultSegment) result).getResult();
            return queryResult.getRows();
        } else {
            return ((MapEntriesWithCursor) result).getBatch();
        }
    }

    private int getNextTableIndexToReadFrom(Object result, boolean queryNeeded) {
        if (queryNeeded) {
            return ((ResultSegment) result).getNextTableIndexToReadFrom();
        } else {
            return ((MapEntriesWithCursor) result).getNextTableIndexToReadFrom();
        }
    }

    private Object toResultSet(ICompletableFuture future) {
        try {
            return future.get();
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

    private ICompletableFuture<ResultSegment> readFromMap(int partitionId, int offset) {
        if (offset < 0) {
            return null;
        }
        Operation op = queryNeeded() ?
                new MapFetchWithQueryOperation(
                        mapName,
                        offset,
                        MAX_FETCH_SIZE,
                        Query.of()
                                .mapName(mapName)
                                .iterationType(IterationType.VALUE)
                                .predicate(predicate)
                                .projection(projection)
                                .build()
                ) :
                new MapFetchEntriesOperation(mapName, offset, MAX_FETCH_SIZE);
        return mapProxy.getOperationService().invokeOnPartition(mapProxy.getServiceName(), op, partitionId);
    }

    private boolean queryNeeded() {
        return predicate != null || projection != null;
    }

    private static class ClusterMetaSupplier<K, V, T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private final Predicate<? super K, ? super V> predicate;
        private final Projection<? super Entry<K, V>, ? extends T> projection;

        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                @Nonnull String mapName,
                @Nullable Predicate<? super K, ? super V> predicate,
                @Nullable Projection<? super Entry<K, V>, ? extends T> projection
        ) {
            this.mapName = mapName;
            this.predicate = predicate;
            this.projection = projection;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
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
            return address -> new ClusterProcessorSupplier<>(mapName, addrToPartitions.get(address),
                    predicate, projection);
        }
    }

    private static class ClusterProcessorSupplier<K, V, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private final List<Integer> ownedPartitions;
        private final Predicate<? super K, ? super V> predicate;
        private final Projection<? super Entry<K, V>, ? extends T> projection;

        private transient BooleanSupplier migrationWatcher;

        ClusterProcessorSupplier(
                @Nonnull String mapName,
                @Nonnull List<Integer> ownedPartitions,
                @Nullable Predicate<? super K, ? super V> predicate,
                @Nullable Projection<? super Entry<K, V>, ? extends T> projection
        ) {
            this.ownedPartitions = ownedPartitions;
            this.mapName = mapName;
            this.predicate = predicate;
            this.projection = projection;
        }

        @Override
        public void init(@Nonnull Context context) {
            Node node = ((HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance()).node;
            JetService jetService = node.nodeEngine.getService(JetService.SERVICE_NAME);
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
            return partitions.isEmpty()
                    ? Processors.noopP().get()
                    : new ReadMapP<>(mapName, partitions, migrationWatcher, predicate, projection);
        }
    }

}
