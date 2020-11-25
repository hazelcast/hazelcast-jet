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
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.kinesis.impl.KinesisHelper.shardBelongsToRange;

public class KinesisSourceP extends AbstractProcessor {

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nonnull
    private final EventTimeMapper<? super Map.Entry<String, byte[]>> eventTimeMapper;
    @Nonnull
    private final HashRange hashRange;

    private int id;
    private ILogger logger;

    private Traverser<Object> traverser = Traversers.empty();

    private KinesisHelper helper;
    private RangeMonitor rangeMonitor;
    private List<ShardReader> shardReaders = new ArrayList<>();
    private int nextReader;

    public KinesisSourceP(
            @Nonnull AmazonKinesisAsync kinesis,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super Map.Entry<String, byte[]>> eventTimePolicy,
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

        runMonitor();
        runReaders();
        return false;
    }

    private void runMonitor() {
        RangeMonitor.Result result = rangeMonitor.run();
        if (RangeMonitor.Result.NEW_SHARDS.equals(result)) {
            Collection<Shard> shards = rangeMonitor.getNewShards();
            addShardReaders(shards);
        }
    }

    private void runReaders() {
        for (int i = 0; i < shardReaders.size(); i++) {
            int currentReader = nextReader;
            ShardReader reader = shardReaders.get(currentReader);
            nextReader = incrCircular(currentReader, shardReaders.size());

            ShardReader.Result result = reader.run();
            if (ShardReader.Result.HAS_DATA.equals(result)) {
                Record[] records = reader.getData();
                traverser = Traversers.traverseArray(records)
                        .flatMap(record -> eventTimeMapper.flatMapEvent(
                                entry(record.getPartitionKey(), record.getData().array()), //todo: shady?
                                currentReader,
                                record.getApproximateArrivalTimestamp().getTime()
                        ));
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

        //todo: actual snapshot saving; we will be saving the sequence numbers of last seen messages, per shard

        //todo: save watermark, see StreamKafkaP
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        //todo: look for last seen sequence numbers of handled shards
        // pass them to readers so that they can request records only from the sequence no. onward

        //todo: restore watermark, see StreamKafkaP
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
        logger.info("Shard " + shard.getShardId() + " of stream " + stream + " assigned to processor instance " + id);
        return new ShardReader(kinesis, stream, shard, logger);
    }

    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }
}
