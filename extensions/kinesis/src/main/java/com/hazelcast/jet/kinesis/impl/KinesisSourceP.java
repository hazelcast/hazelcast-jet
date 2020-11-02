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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;

public class KinesisSourceP extends AbstractProcessor {

    private final AmazonKinesisAsync kinesis;
    private final String stream;
    private final HashRange hashRange;

    private ILogger logger;
    private int processorIndex;
    private List<ShardWorker> workers;
    private Traverser<Object> traverser = Traversers.empty();
    private int workerIndex;

    public KinesisSourceP(AmazonKinesisAsync kinesis, String stream, HashRange hashRange) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.hashRange = hashRange;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);

        logger = context.logger();
        processorIndex = context.globalProcessorIndex();

        workers = getAllShards(kinesis, stream).stream()
                .map(this::toWorker)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private ShardWorker toWorker(Shard shard) {
        if (hashRange.contains(shard.getHashKeyRange().getStartingHashKey())) {
            logger.info("Shard " + shard.getShardId() + " of stream " + stream + " handled by " +
                    KinesisSourceP.class.getSimpleName() + " " + processorIndex);
            return new ShardReadWorker(kinesis, stream, shard.getShardId());
        }
        if (hashRange.contains(shard.getHashKeyRange().getEndingHashKey())) {
            return null; //todo: MonitorWorker, which will watch out for splits
        }
        return null;
    }

    @Override
    public boolean complete() {
        if (workers.isEmpty()) {
            return true; //todo: done, not so when we will have monitor workers too
        }

        if (!emitFromTraverser(traverser)) {
            return false;
        }

        for (int i = 0; i < workers.size(); i++) {
            ShardWorker worker = workers.get(workerIndex);
            workerIndex = incrCircular(workerIndex, workers.size());

            List<Record> records = worker.poll();
            if (!records.isEmpty()) {
                List<String> messages = records.stream()
                        .map(record -> new String(record.getData().array(), Charset.defaultCharset()))
                        .collect(Collectors.toList());
                System.err.println("messages = " + messages); //todo: remove
                traverser = Traversers.traverseIterable(records)
                        .map(r -> entry(r.getPartitionKey(), r.getData().array())); //todo: performance impact
                emitFromTraverser(traverser);
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

    private static List<Shard> getAllShards(AmazonKinesisAsync kinesis, String stream) {
        StreamDescription description = checkStreamActive(stream, kinesis.describeStream(stream).getStreamDescription());
        if (description.getHasMoreShards()) {
            throw new UnsupportedOperationException("Stream with more than 100 shards not yet supported"); //todo
        } else {
            return description.getShards();
        }
    }

    private static StreamDescription checkStreamActive(String stream, StreamDescription description) {
        if (!StreamStatus.ACTIVE.is(description.getStreamStatus())) {
            throw new IllegalStateException("Stream " + stream + " not active"); //todo: better handling; wait? retry?
        }
        return description;
    }

    private static int incrCircular(int v, int limit) {
        v++;
        if (v == limit) {
            v = 0;
        }
        return v;
    }
}
