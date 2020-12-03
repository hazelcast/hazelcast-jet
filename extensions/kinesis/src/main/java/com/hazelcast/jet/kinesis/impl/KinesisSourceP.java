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
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
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
    private final Offsets offsets = new Offsets();

    private int id;
    private ILogger logger;

    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Entry<BroadcastKey<String>, Object[]>> snapshotTraverser;

    private KinesisHelper helper;
    private RangeMonitor rangeMonitor;
    private List<ShardReader> shardReaders = new ArrayList<>();
    private int nextReader;

    public KinesisSourceP(
            @Nonnull AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super Entry<String, byte[]>> eventTimePolicy,
            @Nonnull HashRange hashRange
    ) {
        this.kinesis = Objects.requireNonNull(kinesis, "kinesis");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.hashRange = Objects.requireNonNull(hashRange, "hashRange");
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        logger = context.logger();
        id = context.globalProcessorIndex();

        helper = new KinesisHelper(kinesis, stream, logger);

        logger.info("Processor " + id + " handles " + hashRange);

        helper.waitForStreamToActivate();
        List<Shard> shardsInRange = helper.listShards(
                (Predicate<? super Shard>) shard -> shardBelongsToRange(shard, hashRange));
        rangeMonitor = new RangeMonitor(context.totalParallelism(), kinesis, stream, hashRange, shardsInRange, logger);
        addShardReaders(shardsInRange);
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        long currentTime = System.nanoTime();
        runMonitor(currentTime);
        runReaders(currentTime);
        return false;
    }

    private void runMonitor(long currentTime) {
        Collection<Shard> newShards = rangeMonitor.probe(currentTime);
        if (!newShards.isEmpty()) {
            addShardReaders(newShards);
        }
    }

    private void runReaders(long currentTime) {
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
                offsets.store(reader.getShard().getShardId(), reader.getLastSeenSeqNo(),
                        eventTimeMapper.getWatermark(currentReader));
                emitFromTraverser(traverser);
                return;
            } else if (ShardReader.Result.CLOSED.equals(result)) {
                Shard shard = reader.getShard();
                logger.info("Shard " + shard.getShardId() + " of stream " + stream + " closed");
                removeShardReader(currentReader);
                nextReader = 0;
                return;
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
            snapshotTraverser = traverseStream(offsets.snapshotEntries())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        String shardId = ((BroadcastKey<String>) key).key();

        int readerIndex = getReaderIndex(shardId);
        if (readerIndex >= 0) {
            Object[] values = (Object[]) value;
            String seqNo = (String) values[0];
            Long watermark = (Long) values[1];

            ShardReader reader = shardReaders.get(readerIndex);
            reader.reset(seqNo);

            eventTimeMapper.restoreWatermark(readerIndex, watermark);

            offsets.store(shardId, seqNo, watermark);
        }
    }

    private int getReaderIndex(String shardId) {
        for (int i = 0; i < shardReaders.size(); i++) {
            ShardReader reader = shardReaders.get(i);
            if (reader.getShard().getShardId().equals(shardId)) {
                return i;
            }
        }
        return -1;
    }

    private void addShardReaders(Collection<Shard> shardsInRange) {
        shardReaders.addAll(shardsInRange.stream()
                .map(this::initShardReader)
                .collect(Collectors.toList()));
        eventTimeMapper.addPartitions(shardReaders.size());
    }

    private void removeShardReader(int index) {
        shardReaders.remove(index);
        eventTimeMapper.removePartition(index);
    }

    @Nonnull
    private ShardReader initShardReader(Shard shard) {
        String shardId = shard.getShardId();
        logger.info("Shard " + shardId + " of stream " + stream + " assigned to processor instance " + id);
        return new ShardReader(kinesis, stream, shard, logger);
    }

    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }

    private static class Offsets {

        private final Map<String, Object[]> shardOffsets = new HashMap<>();

        void store(String shardId, String seqNo, long watermark) {
            Object[] offset = shardOffsets.get(shardId);
            if (offset == null) {
                shardOffsets.put(shardId, new Object[] {seqNo, watermark});
            } else {
                offset[0] = seqNo;
                offset[1] = watermark;
            }
        }

        Stream<Entry<BroadcastKey<String>, Object[]>> snapshotEntries() {
            return shardOffsets.entrySet().stream()
                    .map(e -> entry(broadcastKey(e.getKey()), e.getValue()));
        }
    }
}
