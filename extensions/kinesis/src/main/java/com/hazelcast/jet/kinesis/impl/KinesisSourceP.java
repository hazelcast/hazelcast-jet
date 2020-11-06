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
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;

public class KinesisSourceP extends AbstractProcessor {

    @Nonnull
    private final AmazonKinesisAsync kinesis;
    @Nonnull
    private final String stream;
    @Nonnull
    private final HashRange hashRange;

    private int id;
    private ILogger logger;

    private Traverser<Object> traverser = Traversers.empty();

    private RangeMonitor rangeMonitor;
    private List<ShardReader> shardReaders;
    private int nextShardReader;

    public KinesisSourceP(@Nonnull AmazonKinesisAsync kinesis, @Nonnull String stream, @Nonnull HashRange hashRange) {
        this.kinesis = Objects.requireNonNull(kinesis, "kinesis");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.hashRange = Objects.requireNonNull(hashRange, "hashRange");
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        logger = context.logger();
        id = context.globalProcessorIndex();

        List<Shard> shardsInRange = getShardsInRange(kinesis, stream, hashRange);
        rangeMonitor = new RangeMonitor(id, context.totalParallelism(), kinesis, stream, hashRange, shardsInRange, logger);
        shardReaders = shardsInRange.stream()
                .map(this::initShardReader)
                .collect(Collectors.toList());
    }

    @Override
    public boolean complete() {
        if (shardReaders.isEmpty()) {
            return true; //todo: done, not so when we will have monitor workers too
        }

        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (true) { //todo: only do it from time to time
            RangeMonitor.Result result = rangeMonitor.run();
            if (RangeMonitor.Result.NEW_SHARD.equals(result)) {
                Shard shard = rangeMonitor.getNewShard();
                shardReaders.add(initShardReader(shard));
            }
        }

        for (int i = 0; i < shardReaders.size(); i++) {
            ShardReader reader = shardReaders.get(nextShardReader);
            nextShardReader = incrCircular(nextShardReader, shardReaders.size());

            ShardReader.Result result = reader.run();
            if (ShardReader.Result.HAS_DATA.equals(result)) {
                List<Record> records = reader.getData();
                List<String> messages = records.stream()
                        .map(record -> new String(record.getData().array(), Charset.defaultCharset()))
                        .collect(Collectors.toList());
//                System.err.println(i + " - messages = " + messages); //todo: remove
                System.err.println(i + " - messages = " + records.size()); //todo: remove
                traverser = Traversers.traverseIterable(records)
                        .map(r -> entry(r.getPartitionKey(), r.getData().array())); //todo: performance impact
                emitFromTraverser(traverser);
                return false;
            } else if (ShardReader.Result.CLOSED.equals(result)) {
                Shard shard = reader.getShard();
                logger.info("Shard " + shard.getShardId() + " of stream " + stream + " closed");
                shardReaders.remove(decrCircular(nextShardReader, shardReaders.size()));
                rangeMonitor.removeShard(shard);
                nextShardReader = 0;
                return false;
            }
        }

        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        return true; //todo: actual snapshot saving
    }

    @Nonnull
    private ShardReader initShardReader(Shard shard) {
        logger.info("Shard " + shard.getShardId() + " of stream " + stream + " assigned to processor instance " + id);
        return new ShardReader(kinesis, stream, shard);
    }

    private static List<Shard> getShardsInRange(AmazonKinesisAsync kinesis, String stream, HashRange range) {
        return getAllShards(kinesis, stream).stream()
                .filter(shard -> KinesisUtil.shardBelongsToRange(shard, range))
                .collect(Collectors.toList());
    }

    private static List<Shard> getAllShards(AmazonKinesisAsync kinesis, String stream) {
        while (true) {
            StreamDescription description = kinesis.describeStream(stream).getStreamDescription();
            //todo: use list shards instead of describe stream
            String status = description.getStreamStatus();
            if (StreamStatus.ACTIVE.is(status)) {
                return description.getShards();
            } else if (StreamStatus.CREATING.is(status) || StreamStatus.UPDATING.is(status)) {
                sleep(250, TimeUnit.MILLISECONDS); //todo: use exponential backup
            } else if (StreamStatus.DELETING.is(status)) {
                throw new JetException("Stream is being deleted");
            } else {
                throw new RuntimeException("Programming error, unhandled stream status: " + status);
            }
        }
    }

    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }

    private static int decrCircular(int v, int limit) {
        v--;
        if (v < 0) {
            v = limit - 1;
        }
        return v;
    }

    private static void sleep(long duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Waiting for stream to activate interrupted");
        }
    }
}
