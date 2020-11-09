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
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.kinesis.impl.KinesisUtil.shardBelongsToRange;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class RangeMonitor extends AbstractShardWorker {

    /**
     * ListStreams operations are limited to 100 per second, per data stream
     */
    private static final int SHARD_LISTINGS_ALLOWED_PER_SECOND = 100;

    /**
     * We don't want to issue shard listing requests at the peak allowed rate.
     */
    private static final double PERCENTAGE_OF_SHARD_LISTING_RATE_UTILIZED = 0.5;

    private final int id; //todo: remove
    private final HashRange hashRange;
    private final Set<String> knownShards;
    private final RandomizedRateTracker listShardsRateTracker;
    private final ILogger logger;
    private final List<Shard> newShards = new ArrayList<>();

    private State state = State.READY_TO_LIST_SHARDS;
    private String nextToken;
    private Future<ListShardsResult> listShardResult;
    private long nextListShardsTime;

    public RangeMonitor(
            int id,
            int totalInstances,
            AmazonKinesisAsync kinesis,
            String stream,
            HashRange hashRange,
            Collection<Shard> knownShards,
            ILogger logger
    ) {
        super(kinesis, stream);
        this.id = id;
        this.logger = logger;
        this.hashRange = hashRange;
        this.knownShards = knownShards.stream().map(Shard::getShardId).collect(toSet());
        this.listShardsRateTracker = initRandomizedTracker(totalInstances);
        this.nextListShardsTime = System.currentTimeMillis() + listShardsRateTracker.next();
    }

    public Result run() {
        try {
            switch (state) {
                case READY_TO_LIST_SHARDS:
                    if (System.currentTimeMillis() < nextListShardsTime) {
                        return Result.NOTHING;
                    }

                    ListShardsRequest request = KinesisUtil.requestOfShards(stream, nextToken);
                    listShardResult = kinesis.listShardsAsync(request);
                    state = State.WAITING_FOR_SHARD_LIST;

                    nextListShardsTime = System.currentTimeMillis() + listShardsRateTracker.next();

                    return Result.NOTHING;


                case WAITING_FOR_SHARD_LIST:
                    if (listShardResult.isDone()) {
                        try {
                            ListShardsResult result = listShardResult.get();
                            nextToken = result.getNextToken();

                            List<Shard> shards = result.getShards();
                            /*System.err.println(id + " - shards = \n\t" +
                                    shards.stream().map(Object::toString).collect(joining("\n\t")) +
                                    "\nactive = " + KinesisUtil.toString(shards, KinesisUtil::shardIsActive) +
                                    "\nrange = " + hashRange +
                                    "\nin range: " + KinesisUtil.toString(shards,
                                    shard -> shardBelongsToRange(shard, hashRange)));*/ //todo: remove

                            List<Shard> unknownShards = shards.stream()
                                    .filter(KinesisUtil::shardActive)
                                    .filter(shard -> shardBelongsToRange(shard, hashRange))
                                    .filter(shard -> !knownShards.contains(shard.getShardId())).collect(toList());

                            if (unknownShards.isEmpty()) {
                                state = State.READY_TO_LIST_SHARDS;
                                return Result.NOTHING;
                            } else {
//                                System.err.println(id + " - unknownShards = " + unknownShards); //todo: remove
                                knownShards.addAll(unknownShards.stream().map(Shard::getShardId).collect(toList()));
                                newShards.addAll(unknownShards);
                                state = State.NEW_SHARDS_FOUND;
                                return Result.NEW_SHARD;
                            }
                        } catch (ExecutionException e) {
                            logger.warning("Error encountered while reading active shard list. Will retry. Cause: " +
                                    e.getMessage());
                            //todo: exponential backoff?
                            //todo: handle some?
                            nextToken = null;
                            state = State.READY_TO_LIST_SHARDS;
                            return Result.NOTHING;
                        }
                    } else {
                        return Result.NOTHING;
                    }

                case NEW_SHARDS_FOUND:
                    newShards.remove(0);
                    state = newShards.isEmpty() ? State.READY_TO_LIST_SHARDS : State.NEW_SHARDS_FOUND;
                    return newShards.isEmpty() ? Result.NOTHING : Result.NEW_SHARD;

                default:
                    throw new RuntimeException("Programming error, unhandled state: " + state);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    public void removeShard(Shard shard) {
        //todo: how can we clean up the known set?
        // we can't immediately remove them from the known set, when reading data reports them closed, because
        // listing shards is an operation not in sync with reading
        // maybe make the known set shard-last listed timestamp pairs and use that for removing eventually?
    }

    public Shard getNewShard() {
        if (newShards.isEmpty()) {
            throw new IllegalStateException("Can't ask for new shards observed when there are none");
        }
        return newShards.get(0);
    }

    enum Result {
        /**
         * Running the monitor has not produced any events that need handling.
         */
        NOTHING,

        /**
         * Running the monitor has lead to noticing a new shard that needs handling.
         */
        NEW_SHARD
    }

    private enum State {
        /**
         * Ready to request the up-to-date list of shards.
         */
        READY_TO_LIST_SHARDS,

        /**
         * Reading shard list initiated, waiting for the result.
         */
        WAITING_FOR_SHARD_LIST,

        /**
         * Monitor has discovered shards that aren't yet assigned readers.
         */
        NEW_SHARDS_FOUND,
    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int totalInstances) {
        // The maximum rate at which ListStreams operations can be performed on
        // a data stream is 100/second and we need to enforce this, even while
        // we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toMillis(1) * totalInstances,
                (int) (SHARD_LISTINGS_ALLOWED_PER_SECOND * PERCENTAGE_OF_SHARD_LISTING_RATE_UTILIZED));
    }

}
