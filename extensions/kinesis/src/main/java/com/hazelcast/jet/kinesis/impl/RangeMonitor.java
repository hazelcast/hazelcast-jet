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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.kinesis.impl.KinesisHelper.shardBelongsToRange;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
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
    private static final double PERCENTAGE_OF_SHARD_LISTING_RATE_UTILIZED = 0.1;

    /**
     * Failure usually happens due to the over-utilization of resources and/or
     * crossing of various limits. Even if we retry the operation it is a good
     * idea to add some waits (decrease the rate) in order to alleviate the
     * problem.
     */
    private static final long PAUSE_AFTER_FAILURE = SECONDS.toNanos(1); //todo: exponential backoff

    //todo: never removing from the set of known shards, because I have to read from all shards, not
    // just the active ones and I have to not read from shards that are closed and I have read from them already...

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
            int totalInstances,
            AmazonKinesisAsync kinesis,
            String stream,
            HashRange hashRange,
            Collection<Shard> knownShards,
            ILogger logger
    ) {
        super(kinesis, stream, logger);
        this.logger = logger;
        this.hashRange = hashRange;
        this.knownShards = knownShards.stream().map(Shard::getShardId).collect(toSet());
        this.listShardsRateTracker = initRandomizedTracker(totalInstances);
        this.nextListShardsTime = System.nanoTime() + listShardsRateTracker.next();
    }

    public Result run() {
        switch (state) {
            case READY_TO_LIST_SHARDS:
                return handleReadyToListShards();
            case WAITING_FOR_SHARD_LIST:
                return handleWaitingForShardList();
            case NEW_SHARDS_FOUND:
                return handleNewShardsFound();
            default:
                throw new RuntimeException("Programming error, unhandled state: " + state);
        }
    }

    private Result handleReadyToListShards() {
        if (System.nanoTime() < nextListShardsTime) {
            return Result.NOTHING;
        }

        listShardResult = helper.listShardsAsync(nextToken);
        state = State.WAITING_FOR_SHARD_LIST;

        nextListShardsTime = System.nanoTime() + listShardsRateTracker.next();

        return Result.NOTHING;
    }

    private Result handleWaitingForShardList() {
        if (listShardResult.isDone()) {
            try {
                ListShardsResult result = helper.readResult(listShardResult);
                nextToken = result.getNextToken();

                List<Shard> shards = result.getShards();

                List<Shard> unknownShards = shards.stream()
                        .filter(shard -> shardBelongsToRange(shard, hashRange))
                        .filter(shard -> !knownShards.contains(shard.getShardId())).collect(toList());

                if (unknownShards.isEmpty()) {
                    state = State.READY_TO_LIST_SHARDS;
                    return Result.NOTHING;
                } else {
                    knownShards.addAll(unknownShards.stream().map(Shard::getShardId).collect(toList()));
                    newShards.addAll(unknownShards);
                    logger.info("New shards detected: " +
                            unknownShards.stream().map(Shard::getShardId).collect(joining(", ")));
                    state = State.NEW_SHARDS_FOUND;
                    return Result.NEW_SHARDS;
                }
            } catch (SdkClientException e) {
                logger.warning("Failed listing shards, retrying. Cause: " + e.getMessage());
                nextToken = null;
                nextListShardsTime = System.nanoTime() + PAUSE_AFTER_FAILURE;
                state = State.READY_TO_LIST_SHARDS;
                return Result.NOTHING;
            } catch (Throwable t) {
                throw rethrow(t);
            }
        } else {
            return Result.NOTHING;
        }
    }

    private Result handleNewShardsFound() {
        newShards.clear();
        state = State.READY_TO_LIST_SHARDS;
        return Result.NOTHING;
    }

    public Collection<Shard> getNewShards() {
        if (newShards.isEmpty()) {
            throw new IllegalStateException("Can't ask for new shards observed when there are none");
        }
        return newShards;
    }

    enum Result {
        /**
         * Running the monitor has not produced any events that need handling.
         */
        NOTHING,

        /**
         * Running the monitor has lead to noticing new shards that need handling.
         */
        NEW_SHARDS
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
        return new RandomizedRateTracker(SECONDS.toNanos(1) * totalInstances,
                (int) (SHARD_LISTINGS_ALLOWED_PER_SECOND * PERCENTAGE_OF_SHARD_LISTING_RATE_UTILIZED));
    }

}