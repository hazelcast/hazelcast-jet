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
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.List;
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
    private final HashRange hashRange;

    private int id;
    private ILogger logger;

    private Traverser<Object> traverser = Traversers.empty();

    private KinesisHelper helper;
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

        helper = new KinesisHelper(kinesis, stream, logger);

        logger.info("Processor " + id + " handles " + hashRange);

        helper.waitForStreamToActivate();
        List<Shard> shardsInRange = helper.listActiveShards(
                (Predicate<? super Shard>) shard -> shardBelongsToRange(shard, hashRange));
        rangeMonitor = new RangeMonitor(context.totalParallelism(), kinesis, stream, hashRange, shardsInRange, logger);
        shardReaders = shardsInRange.stream()
                .map(this::initShardReader)
                .collect(Collectors.toList());
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
        if (RangeMonitor.Result.NEW_SHARD.equals(result)) {
            Shard shard = rangeMonitor.getNewShard();
            shardReaders.add(initShardReader(shard));
        }
    }

    private void runReaders() {
        for (int i = 0; i < shardReaders.size(); i++) {
            ShardReader reader = shardReaders.get(nextShardReader);
            nextShardReader = incrCircular(nextShardReader, shardReaders.size());

            ShardReader.Result result = reader.run();
            if (ShardReader.Result.HAS_DATA.equals(result)) {
                Record[] records = reader.getData();
                //System.err.println(reader.getShard().getShardId() + " - messages = " + records.length); //todo: remove
                traverser = Traversers.traverseArray(records)
                        .map(r -> entry(r.getPartitionKey(), r.getData().array())); //todo: shady
                emitFromTraverser(traverser);
                return;
            } else if (ShardReader.Result.CLOSED.equals(result)) {
                Shard shard = reader.getShard();
                logger.info("Shard " + shard.getShardId() + " of stream " + stream + " closed");
                shardReaders.remove(decrCircular(nextShardReader, shardReaders.size()));
                rangeMonitor.removeShard(shard);
                nextShardReader = 0;
                return;
            }
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        //todo: actual snapshot saving; we will be saving the sequence numbers of last seen messages, per shard
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        //todo: look for last seen sequence numbers of handled shards
        // pass them to readers so that they can request records only from the sequence no. onward
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

    private static int decrCircular(int v, int limit) {
        v--;
        if (v < 0) {
            v = limit - 1;
        }
        return v;
    }
}
