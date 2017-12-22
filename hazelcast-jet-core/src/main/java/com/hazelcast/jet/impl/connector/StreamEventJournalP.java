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

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JournalInitialPosition;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.util.function.Predicate;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

/**
 * @see SourceProcessors#streamMapP(String, DistributedPredicate, DistributedFunction, JournalInitialPosition)
 */
public final class StreamEventJournalP<E, T> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 128;

    private final EventJournalReader<E> eventJournalReader;
    /**
     * Index is the real HZ partition number, value is the 0-based partition index or -1,
     * if that partition is not assigned to us.
     */
    private final int[] assignedPartitionsIndexes;
    private final List<Integer> assignedPartitions;
    private final Predicate<E> predicate;
    private final Projection<E, T> projection;
    private final JournalInitialPosition initialPos;
    private final boolean isRemoteReader;
    private final WatermarkSourceUtil<T> watermarkSourceUtil;

    // keep track of next offset to emit and read separately, as even when the
    // outbox is full we can still poll for new items.
    private final Map<Integer, Long> emitOffsets = new HashMap<>();
    private final Map<Integer, Long> readOffsets = new HashMap<>();

    private final Map<Integer, ICompletableFuture<ReadResultSet<T>>> readFutures = new HashMap<>();

    private Traverser<Object> outputTraverser;
    private Traverser<Entry<BroadcastKey<Object>, Object>> snapshotTraverser;

    // keep track of pendingItem's offset and partition
    private long pendingItemOffset;
    private int pendingItemPartition;

    // callback which will update the currently pending offset only after the item is emitted
    private Consumer<Object> updateOffsetFn = eventOrWm -> emitOffsets.put(pendingItemPartition, pendingItemOffset + 1);
    private Iterator<Entry<Integer, ICompletableFuture<ReadResultSet<T>>>> iterator;

    StreamEventJournalP(@Nonnull EventJournalReader<E> eventJournalReader,
                        @Nonnull List<Integer> assignedPartitions,
                        @Nonnull DistributedPredicate<E> predicateFn,
                        @Nonnull DistributedFunction<E, T> projectionFn,
                        @Nonnull JournalInitialPosition initialPos,
                        boolean isRemoteReader,
                        @Nonnull DistributedToLongFunction<T> getTimestampF,
                        @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
                        @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
                        long idleTimeoutMillis) {
        this.eventJournalReader = eventJournalReader;
        this.predicate = (Serializable & Predicate<E>) predicateFn::test;
        this.projection = toProjection(projectionFn);
        this.initialPos = initialPos;
        this.isRemoteReader = isRemoteReader;

        this.assignedPartitions = assignedPartitions;
        this.assignedPartitionsIndexes = new int[assignedPartitions.stream().mapToInt(Integer::intValue).max().orElse(0) + 1];
        Arrays.fill(this.assignedPartitionsIndexes, -1);
        for (int i = 0; i < assignedPartitions.size(); i++) {
            this.assignedPartitionsIndexes[assignedPartitions.get(i)] = i;
        }

        // TODO configure timeout
        watermarkSourceUtil = new WatermarkSourceUtil<>(assignedPartitions.size(), idleTimeoutMillis, getTimestampF,
                newWmPolicyF, wmEmitPolicy);
    }

    @Override
    protected void init(@Nonnull Context context) {
        Map<Integer, ICompletableFuture<EventJournalInitialSubscriberState>> futures = IntStream.of(assignedPartitionsIndexes)
            .filter(i -> i >= 0)
            .mapToObj(partition -> entry(partition, eventJournalReader.subscribeToEventJournal(partition)))
            .collect(toMap(Entry::getKey, Entry::getValue));
        futures.forEach((partition, future) -> uncheckRun(() -> readOffsets.put(partition, getSequence(future.get()))));
        emitOffsets.putAll(readOffsets);
    }

    @Override
    public boolean complete() {
        if (readFutures.isEmpty()) {
            initialRead();
        }
        if (outputTraverser == null) {
            Traverser<Object> t = nextTraverser();
            if (t != null) {
                outputTraverser = t.onFirstNull(() -> outputTraverser = null);
            }
        }

        if (outputTraverser != null) {
            emitFromTraverser(outputTraverser, updateOffsetFn);
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            Traverser<Entry<BroadcastKey<Object>, Object>> ourTraverser =
                    traverseIterable(emitOffsets.entrySet())
                            .map(e -> entry(broadcastKey(e.getKey()), e.getValue()));

            Traverser<Entry<BroadcastKey<Object>, Object>> wsuTraverser =
                    watermarkSourceUtil.saveToSnapshot(i -> "wm" + assignedPartitions.get(i));

            snapshotTraverser = Traverser.over(ourTraverser, wsuTraverser)
                    .flatMap(Function.identity())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        boolean done = emitFromTraverserToSnapshot(snapshotTraverser);
        if (done) {
            logFinest(getLogger(), "Saved snapshot. Offsets: %s", emitOffsets);
        }
        return done;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        Object unwrappedKey = ((BroadcastKey<Object>) key).key();
        // Integer key is our emit offset, String key is whatever watermarkSourceUtil saved
        if (unwrappedKey instanceof Integer) {
            int partition = (int) unwrappedKey;
            long offset = (Long) value;
            if (assignedPartitionsIndexes[partition] >= 0) {
                readOffsets.put(partition, offset);
                emitOffsets.put(partition, offset);
            }
        } else if (unwrappedKey instanceof String) {
            assert ((String) unwrappedKey).startsWith("wm") : "unexpected key: " + unwrappedKey;
            int partition = Integer.parseInt(((String) unwrappedKey).substring(2));
            watermarkSourceUtil.restoreFromSnapshot(assignedPartitionsIndexes[partition], value);
        } else {
            throw new JetException("Unexpected snapshot key: " + key);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        logFinest(getLogger(), "Restored snapshot. Offsets: %s", readOffsets);
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void initialRead() {
        readOffsets.forEach((partition, offset) ->
                readFutures.put(partition, readFromJournal(partition, offset)));
        iterator = readFutures.entrySet().iterator();
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        return initialPos == START_FROM_CURRENT ? state.getNewestSequence() + 1 : state.getOldestSequence();
    }

    private Traverser<Object> nextTraverser() {
        ReadResultSet<T> resultSet = nextResultSet();
        if (resultSet == null) {
            Watermark wm = watermarkSourceUtil.handleNoEvent();
            return wm != null ? Traverser.over(wm) : null;
        }

        Traverser<T> traverser = traverseIterable(resultSet);

        return peekIndex(traverser, i -> pendingItemOffset = resultSet.getSequence(i))
                .flatMap(event -> watermarkSourceUtil.flatMapEvent(event, assignedPartitionsIndexes[pendingItemPartition]));
    }

    private ReadResultSet<T> nextResultSet() {
        while (iterator.hasNext()) {
            Entry<Integer, ICompletableFuture<ReadResultSet<T>>> entry = iterator.next();
            int partition = entry.getKey();
            if (!entry.getValue().isDone()) {
                continue;
            }
            ReadResultSet<T> resultSet = toResultSet(partition, entry.getValue());
            if (resultSet == null || resultSet.size() == 0) {
                // we got stale sequence or empty response, make another read
                entry.setValue(readFromJournal(partition, readOffsets.get(partition)));
                continue;
            }
            pendingItemPartition = partition;
            long newOffset = readOffsets.merge(partition, (long) resultSet.readCount(), Long::sum);
            // make another read on the same partition
            entry.setValue(readFromJournal(partition, newOffset));
            return resultSet;
        }
        iterator = readFutures.entrySet().iterator();
        return null;
    }

    private ReadResultSet<T> toResultSet(int partition, ICompletableFuture<ReadResultSet<T>> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastInstanceNotActiveException && !isRemoteReader) {
                // This exception can be safely ignored - it means the instance was shutting down,
                // so we shouldn't unnecessarily throw an exception here.
                return null;
            } else if (ex instanceof StaleSequenceException) {
                long headSeq = ((StaleSequenceException) e.getCause()).getHeadSeq();
                // move both read and emitted offsets to the new head
                long oldOffset = readOffsets.put(partition, headSeq);
                emitOffsets.put(partition, headSeq);
                getLogger().warning("Events lost for partition " + partition + " due to journal overflow " +
                        "when reading from event journal. Increase journal size to avoid this error. " +
                        "Requested was: " + oldOffset + ", current head is: " + headSeq);
                return null;
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private ICompletableFuture<ReadResultSet<T>> readFromJournal(int partition, long offset) {
        logFine(getLogger(), "Reading from partition %s and offset %s", partition, offset);
        return eventJournalReader.readFromEventJournal(offset,
                1, MAX_FETCH_SIZE, partition, predicate, projection);
    }

    /**
     * Returns a new traverser, that will return same items as supplied {@code
     * traverser} and before each item is returned a zero-based index is sent
     * to the {@code action}.
     */
    private static <T> Traverser<T> peekIndex(Traverser<T> traverser, IntConsumer action) {
        int[] count = {0};
        return () -> {
            T t = traverser.next();
            if (t != null) {
                action.accept(count[0]++);
            }
            return t;
        };
    }

    private static <E, T> Projection<E, T> toProjection(Function<E, T> projectionFn) {
        return new Projection<E, T>() {
            @Override
            public T transform(E input) {
                return projectionFn.apply(input);
            }
        };
    }

    private static class ClusterMetaSupplier<E, T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig serializableConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final DistributedPredicate<E> predicate;
        private final DistributedFunction<E, T> projection;
        private final JournalInitialPosition initialPos;
        private final DistributedToLongFunction<T> getTimestampF;
        private final DistributedSupplier<WatermarkPolicy> newWmPolicyF;
        private final WatermarkEmissionPolicy wmEmitPolicy;
        private final long idleTimeoutMillis;

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                ClientConfig clientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                JournalInitialPosition initialPos,
                DistributedToLongFunction<T> getTimestampF,
                DistributedSupplier<WatermarkPolicy> newWmPolicyF,
                WatermarkEmissionPolicy wmEmitPolicy,
                long idleTimeoutMillis) {
            this.serializableConfig = clientConfig == null ? null : new SerializableClientConfig(clientConfig);
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
            this.getTimestampF = getTimestampF;
            this.newWmPolicyF = newWmPolicyF;
            this.wmEmitPolicy = wmEmitPolicy;
            this.idleTimeoutMillis = idleTimeoutMillis;
        }

        @Override
        public int preferredLocalParallelism() {
            return serializableConfig != null ? 1 : 2;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (serializableConfig != null) {
                initRemote();
            } else {
                initLocal(context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions());
            }
        }

        private void initRemote() {
            HazelcastInstance client = newHazelcastClient(serializableConfig.asClientConfig());
            try {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
                remotePartitionCount = clientProxy.client.getClientPartitionService().getPartitionCount();
            } finally {
                client.shutdown();
            }
        }

        private void initLocal(Set<Partition> partitions) {
            addrToPartitions = partitions.stream()
                                         .collect(groupingBy(p -> p.getOwner().getAddress(),
                                                 mapping(Partition::getPartitionId, toList())));
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (addrToPartitions == null) {
                // assign each remote partition to a member
                addrToPartitions = range(0, remotePartitionCount)
                        .boxed()
                        .collect(groupingBy(partition -> addresses.get(partition % addresses.size())));
            }

            return address -> new ClusterProcessorSupplier<>(addrToPartitions.get(address),
                    serializableConfig, eventJournalReaderSupplier, predicate, projection, initialPos,
                    getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
        }

    }

    private static class ClusterProcessorSupplier<E, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<Integer> ownedPartitions;
        private final SerializableClientConfig serializableClientConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final DistributedPredicate<E> predicate;
        private final DistributedFunction<E, T> projection;
        private final JournalInitialPosition initialPos;
        private final DistributedToLongFunction<T> getTimestampF;
        private final DistributedSupplier<WatermarkPolicy> newWmPolicyF;
        private final WatermarkEmissionPolicy wmEmitPolicy;
        private final long idleTimeoutMillis;

        private transient HazelcastInstance client;
        private transient EventJournalReader<E> eventJournalReader;

        ClusterProcessorSupplier(
                List<Integer> ownedPartitions,
                SerializableClientConfig serializableClientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                JournalInitialPosition initialPos,
                DistributedToLongFunction<T> getTimestampF,
                DistributedSupplier<WatermarkPolicy> newWmPolicyF,
                WatermarkEmissionPolicy wmEmitPolicy, long idleTimeoutMillis) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
            this.getTimestampF = getTimestampF;
            this.newWmPolicyF = newWmPolicyF;
            this.wmEmitPolicy = wmEmitPolicy;
            this.idleTimeoutMillis = idleTimeoutMillis;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance = context.jetInstance().getHazelcastInstance();
            if (serializableClientConfig != null) {
                client = newHazelcastClient(serializableClientConfig.asClientConfig());
                instance = client;
            }
            eventJournalReader = eventJournalReaderSupplier.apply(instance);
        }

        @Override
        public void complete(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return processorToPartitions(count, ownedPartitions)
                    .values().stream()
                    .map(this::processorForPartitions)
                    .collect(toList());
        }

        private Processor processorForPartitions(List<Integer> partitions) {
            return partitions.isEmpty()
                    ? Processors.noopP().get()
                    : new StreamEventJournalP<>(eventJournalReader, partitions, predicate, projection,
                    initialPos, client != null, getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis) {
        return new ClusterMetaSupplier<>(null,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, initialPos, getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis) {
        return new ClusterMetaSupplier<>(clientConfig,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, initialPos, getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis) {
        return new ClusterMetaSupplier<>(null,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, initialPos, getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis) {
        return new ClusterMetaSupplier<>(clientConfig,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, initialPos, getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
    }
}
