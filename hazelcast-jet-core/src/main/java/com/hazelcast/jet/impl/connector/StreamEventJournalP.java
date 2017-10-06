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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.StaleSequenceException;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
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
 * @see SourceProcessors#streamMapP(String, DistributedPredicate, DistributedFunction, boolean)
 */
public final class StreamEventJournalP<E, T> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 128;

    private final EventJournalReader<E> eventJournalReader;
    private final Set<Integer> assignedPartitions;
    private final SerializablePredicate<E> predicateFn;
    private final Projection<E, T> projectionFn;
    private final boolean startFromNewest;

    // keep track of emitted and read offsets separately, as even when the
    // outbox is full we can still poll for new items.
    private final Map<Integer, Long> emittedOffsets = new HashMap<>();
    private final Map<Integer, Long> readOffsets = new HashMap<>();

    private Map<Integer, ICompletableFuture<ReadResultSet<T>>> readFutures;
    private Map<Integer, EventJournalInitialSubscriberState> subscriptions = new HashMap<>();

    private Traverser<T> eventTraverser;
    private Traverser<Entry<BroadcastKey<Integer>, Long>> snapshotTraverser;

    // keep track of pendingItem's offset and partition
    private long pendingOffset;
    private int pendingPartition;

    // callback which will update the currently pending offset only after the item is emitted
    private Consumer<T> updateOffsetFn = e -> emittedOffsets.put(pendingPartition, pendingOffset);

    StreamEventJournalP(EventJournalReader<E> eventJournalReader,
                        List<Integer> assignedPartitions,
                        DistributedPredicate<E> predicateFn,
                        DistributedFunction<E, T> projectionFn,
                        boolean startFromNewest) {
        this.eventJournalReader = eventJournalReader;
        this.assignedPartitions = new HashSet<>(assignedPartitions);
        this.predicateFn = predicateFn == null ? null : predicateFn::test;
        this.projectionFn = projectionFn == null ? null : toProjection(projectionFn);
        this.startFromNewest = startFromNewest;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        Map<Integer, ICompletableFuture<EventJournalInitialSubscriberState>> futures = assignedPartitions.stream()
            .map(partition -> entry(partition, eventJournalReader.subscribeToEventJournal(partition)))
            .collect(toMap(Entry::getKey, Entry::getValue));
        futures.forEach((partition, future) -> uncheckRun(() -> subscriptions.put(partition, future.get())));
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        if (eventTraverser == null) {
            Traverser<T> t = nextTraverser();
            if (t != null) {
                eventTraverser = t.onFirstNull(() -> eventTraverser = null);
            }
        }

        if (eventTraverser != null) {
            emitFromTraverser(eventTraverser, item -> {
                updateOffsetFn.accept(item);
            });
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(emittedOffsets.entrySet())
                    .map(e -> entry(broadcastKey(e.getKey()), e.getValue()))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        boolean done = emitFromTraverserToSnapshot(snapshotTraverser);
        if (done) {
            logFinest(getLogger(), "Saved snapshot. Offsets: %s", emittedOffsets);
        }
        return done;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        int partition = ((BroadcastKey<Integer>) key).key();
        long offset = (Long) value;

        if (assignedPartitions.contains(partition)) {
            // after snapshot restore, we try to continue where we left off. However the
            // current oldest sequence might be greater than the restored offset. This would cause
            // stale sequence exception, so we fast forward to the oldest sequence instead.
            long oldest = subscriptions.get(partition).getOldestSequence();
            long newOffset = Math.max(oldest, offset);
            if (newOffset != offset) {
                getLogger().warning("Events lost for partition " + partition + " when restoring from " +
                        "snapshot. Requested was: " + offset + ", current head is: " + oldest);
            }
            readOffsets.put(partition, newOffset);
            emittedOffsets.put(partition, newOffset);
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
        if (readOffsets.isEmpty()) {
            subscriptions.forEach((partition, future) ->
                    uncheckRun(() -> readOffsets.put(partition, getSequence(subscriptions.get(partition))))
            );
            emittedOffsets.putAll(readOffsets);
        }
        readFutures = readOffsets.entrySet().stream()
            .collect(toMap(Entry::getKey, e -> readFromJournal(e.getKey(), e.getValue())));
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        return startFromNewest ? state.getNewestSequence() + 1 : state.getOldestSequence();
    }

    private Traverser<T> nextTraverser() {
        ReadResultSet<T> resultSet = nextResultSet();
        if (resultSet == null) {
            return null;
        }
        Traverser<T> traverser = traverseIterable(resultSet);
        final ReadResultSet<T> currentSet = resultSet;
        return peekIndex(traverser, i -> {
            pendingOffset = currentSet.getSequence(i) + 1;
        });
    }

    private ReadResultSet<T> nextResultSet() {
        Iterator<Entry<Integer, ICompletableFuture<ReadResultSet<T>>>> iterator = readFutures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Integer, ICompletableFuture<ReadResultSet<T>>> entry = iterator.next();
            int partition = entry.getKey();
            if (!entry.getValue().isDone()) {
                continue;
            }
            ReadResultSet<T> resultSet = toResultSet(partition, entry.getValue());
            if (resultSet == null) {
                // we got stale sequence, make another read
                readFutures.put(partition, readFromJournal(partition, readOffsets.get(partition)));
                continue;
            }
            pendingPartition = partition;
            long newOffset = readOffsets.merge(partition, (long) resultSet.readCount(), Long::sum);
            // make another read on the same partition
            readFutures.put(partition, readFromJournal(partition, newOffset));
            return resultSet;
        }
        return null;
    }

    private ReadResultSet<T> toResultSet(int partition, ICompletableFuture<ReadResultSet<T>> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof StaleSequenceException) {
                long headSeq = ((StaleSequenceException) e.getCause()).getHeadSeq();
                // move both read and emitted offsets to the new head
                long oldOffset = readOffsets.put(partition, headSeq);
                emittedOffsets.put(partition, headSeq);
                getLogger().warning("Events lost for partition " + partition + " due to journal overflow " +
                        "when reading from event journal. Increase journal size to avoid this error. " +
                        "Requested was: " + oldOffset + ", current head is: " + headSeq);
                return null;
            }
            throw rethrow(ex);
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private ICompletableFuture<ReadResultSet<T>> readFromJournal(int partition, long offset) {
        logFine(getLogger(), "Reading from partition %s and offset %s", partition, offset);
        return eventJournalReader.readFromEventJournal(offset,
                1, MAX_FETCH_SIZE, partition, predicateFn, projectionFn);
    }

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
        private final boolean startFromNewest;

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                ClientConfig clientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                boolean startFromNewest) {
            this.serializableConfig = clientConfig == null ? null : new SerializableClientConfig(clientConfig);
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.startFromNewest = startFromNewest;
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
                    serializableConfig, eventJournalReaderSupplier, predicate, projection, startFromNewest);
        }

    }

    private static class ClusterProcessorSupplier<E, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<Integer> ownedPartitions;
        private final SerializableClientConfig serializableClientConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final DistributedPredicate<E> predicate;
        private final DistributedFunction<E, T> projection;
        private final boolean startFromNewest;

        private transient HazelcastInstance client;
        private transient EventJournalReader<E> eventJournalReader;

        ClusterProcessorSupplier(
                List<Integer> ownedPartitions,
                SerializableClientConfig serializableClientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                boolean startFromNewest) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.startFromNewest = startFromNewest;
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
            return !partitions.isEmpty() ?
                    new StreamEventJournalP<>(eventJournalReader, partitions, predicate, projection, startFromNewest)
                    :
                    Processors.noopP().get();
        }

    }

    public static <K, V, T> ProcessorMetaSupplier streamMap(String mapName,
                                                      DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
                                                      DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
                                                      boolean startFromNewest) {
        return new ClusterMetaSupplier<>(null,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, startFromNewest);
    }

    public static <K, V, T> ProcessorMetaSupplier streamMap(String mapName,
                                                      ClientConfig clientConfig,
                                                      DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
                                                      DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
                                                      boolean startFromNewest) {
        return new ClusterMetaSupplier<>(clientConfig,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, startFromNewest);
    }

    public static <K, V, T> ProcessorMetaSupplier streamCache(String cacheName,
                                                        DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
                                                        DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
                                                        boolean startFromNewest) {
        return new ClusterMetaSupplier<>(null,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, startFromNewest);
    }

    public static <K, V, T> ProcessorMetaSupplier streamCache(String cacheName,
                                                        ClientConfig clientConfig,
                                                        DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
                                                        DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
                                                        boolean startFromNewest) {
        return new ClusterMetaSupplier<>(clientConfig,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, startFromNewest);
    }

    interface SerializablePredicate<E> extends com.hazelcast.util.function.Predicate<E>, Serializable {
    }
}
