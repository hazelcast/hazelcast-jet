/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.kinesis.impl.KinesisHelper.shardBelongsToRange;

public class KinesisSourceP extends AbstractProcessor {

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nonnull
    private final EventTimeMapper<? super Entry<String, byte[]>> eventTimeMapper;
    @Nonnull
    private final HashRange hashRange;
    @Nonnull
    private final ShardStates shardStates = new ShardStates();
    @Nonnull
    private final Queue<Shard> shardQueue;
    @Nullable
    private final RangeMonitor monitor;
    @Nonnull
    private final List<ShardReader> shardReaders = new ArrayList<>();
    @Nonnull
    private final RetryStrategy retryStrategy;

    private int id;
    private ILogger logger;

    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Entry<BroadcastKey<String>, Object[]>> snapshotTraverser;

    private int nextReader;

    public KinesisSourceP(
            @Nonnull AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super Entry<String, byte[]>> eventTimePolicy,
            @Nonnull HashRange hashRange,
            @Nonnull Queue<Shard> shardQueue,
            @Nullable RangeMonitor monitor,
            @Nonnull RetryStrategy retryStrategy
            ) {
        this.kinesis = Objects.requireNonNull(kinesis, "kinesis");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.hashRange = Objects.requireNonNull(hashRange, "hashRange");
        this.shardQueue = shardQueue;
        this.monitor = monitor;
        this.retryStrategy = retryStrategy;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        logger = context.logger();
        id = context.globalProcessorIndex();

        logger.info("Processor " + id + " handles " + hashRange);
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        runMonitor();
        checkForNewShards();
        runReaders();

        return false;
    }

    private void runMonitor() {
        if (monitor != null) {
            monitor.run();
        }
    }

    private void checkForNewShards() {
        Shard shard = shardQueue.poll();
        if (shard != null) {
            addShardReader(shard);
        }
    }

    private void runReaders() {
        if (!shardReaders.isEmpty()) {
            long currentTime = System.nanoTime();
            for (int i = 0; i < shardReaders.size(); i++) {
                int currentReader = nextReader;
                ShardReader reader = shardReaders.get(currentReader);
                nextReader = incrCircular(currentReader, shardReaders.size());

                ShardReader.Result result = reader.probe(currentTime);
                if (ShardReader.Result.HAS_DATA.equals(result)) {
                    traverser = reader.clearData()
                            .flatMap(record -> eventTimeMapper.flatMapEvent(
                                    entry(record.getPartitionKey(), record.getData().array()), //todo: shady?
                                    currentReader,
                                    record.getApproximateArrivalTimestamp().getTime()
                            ));
                    Long watermark = eventTimeMapper.getWatermark(currentReader);
                    watermark = watermark < 0 ? null : watermark;
                    shardStates.update(reader.getShard(), reader.getLastSeenSeqNo(), watermark);
                    emitFromTraverser(traverser);
                    return;
                } else if (ShardReader.Result.CLOSED.equals(result)) {
                    Shard shard = reader.getShard();
                    logger.info("Shard " + shard.getShardId() + " of stream " + stream + " closed");
                    shardStates.close(shard);
                    removeShardReader(currentReader);
                    nextReader = 0;
                    return;
                }
            }
        }

        traverser = eventTimeMapper.flatMapIdle();
        emitFromTraverser(traverser);
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            snapshotTraverser = traverseStream(shardStates.snapshotEntries())
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        if (getLogger().isFinestEnabled()) {
                            getLogger().finest("Finished saving snapshot. Saved shard states: " + shardStates);
                        }
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        String shardId = ((BroadcastKey<String>) key).key();

        Object[] shardState = (Object[]) value;
        String startingHashKey = ShardStates.startingHashKey(shardState);
        if (shardBelongsToRange(startingHashKey, hashRange)) {
            boolean closed = ShardStates.closed(shardState);
            String seqNo = ShardStates.lastSeenSeqNo(shardState);
            Long watermark = ShardStates.watermark(shardState);
            shardStates.update(shardId, startingHashKey, closed, seqNo, watermark);
        }
    }

    private void addShardReader(Shard shard) {
        String shardId = shard.getShardId();
        Object[] shardState = shardStates.get(shardId);
        if (!ShardStates.closed(shardState)) {
            int readerIndex = shardReaders.size();

            String lastSeenSeqNo = ShardStates.lastSeenSeqNo(shardState);
            shardReaders.add(initShardReader(shard, lastSeenSeqNo));

            eventTimeMapper.addPartitions(1);

            Long watermark = ShardStates.watermark(shardState);
            if (watermark != null) {
                eventTimeMapper.restoreWatermark(readerIndex, watermark);
            }
        }
    }

    private void removeShardReader(int index) {
        shardReaders.remove(index);
        eventTimeMapper.removePartition(index);
    }

    @Nonnull
    private ShardReader initShardReader(Shard shard, String lastSeenSeqNo) {
        logger.info("Shard " + shard.getShardId() + " of stream " + stream + " assigned to processor instance " + id);
        return new ShardReader(kinesis, stream, shard, lastSeenSeqNo, retryStrategy, logger);
    }

    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }

    private static class ShardStates {

        private static final int STATE_LENGTH = 4;

        private static final int STARTING_HASH_KEY_INDEX = 0;
        private static final int IS_CLOSED_INDEX = 1;
        private static final int LAST_SEEN_SEQ_NO_INDEX = 2;
        private static final int WATERMARK_INDEX = 3;

        private static final Object[] NO_STATE = new Object[STATE_LENGTH];

        private final Map<String, Object[]> states = new HashMap<>();

        void update(Shard shard, String seqNo, Long watermark) {
            update(shard.getShardId(), shard.getHashKeyRange().getStartingHashKey(), false, seqNo, watermark);
        }

        void close(Shard shard) {
            update(shard.getShardId(), shard.getHashKeyRange().getStartingHashKey(), true, null, null);
        }

        void update(String shardId, String startingHashKey, boolean closed, String lastSeenSeqNo, Long watermark) {
            Object[] stateValues = states.computeIfAbsent(shardId, IGNORED -> new Object[STATE_LENGTH]);
            stateValues[STARTING_HASH_KEY_INDEX] = startingHashKey;
            stateValues[IS_CLOSED_INDEX] = closed;
            stateValues[LAST_SEEN_SEQ_NO_INDEX] = lastSeenSeqNo;
            stateValues[WATERMARK_INDEX] = watermark;
        }

        Object[] get(String shardId) {
            return states.getOrDefault(shardId, NO_STATE);
        }

        Stream<Entry<BroadcastKey<String>, Object[]>> snapshotEntries() {
            return states.entrySet().stream()
                    .map(e -> entry(broadcastKey(e.getKey()), e.getValue()));
        }

        @Override
        public String toString() {
            return states.entrySet().stream().map(this::toString).collect(Collectors.joining(", "));
        }

        private String toString(Map.Entry<String, Object[]> entry) {
            StringBuilder sb = new StringBuilder();

            String shardId = entry.getKey();
            sb.append(shardId).append(": ");

            Object[] state = entry.getValue();

            String startingHashKey = startingHashKey(state);
            sb.append("startingHashKey=").append(startingHashKey);

            boolean closed = closed(state);
            sb.append(", closed=").append(closed);

            if (!closed) {
                String lastSeenSeqNo = lastSeenSeqNo(state);
                sb.append(", lastSeenSeqNo=").append(lastSeenSeqNo);

                Long watermark = watermark(state);
                if (watermark != null) {
                    sb.append(", watermark=").append(toLocalTime(watermark));
                }
            }

            return sb.toString();
        }

        static String startingHashKey(Object[] state) {
            return (String) state[STARTING_HASH_KEY_INDEX];
        }

        static boolean closed(Object[] state) {
            Boolean closed = (Boolean) state[IS_CLOSED_INDEX];
            return closed != null && closed;
        }

        static String lastSeenSeqNo(Object[] state) {
            return (String) state[LAST_SEEN_SEQ_NO_INDEX];
        }

        static Long watermark(Object[] state) {
            return (Long) state[WATERMARK_INDEX];
        }
    }
}
