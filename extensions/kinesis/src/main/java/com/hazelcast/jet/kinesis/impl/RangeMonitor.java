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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.kinesis.impl.KinesisHelper.shardBelongsToRange;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class RangeMonitor extends AbstractShardWorker {

    /**
     * ListStreams operations are limited to 100 per second, per data stream
     */
    private static final int SHARD_LISTINGS_ALLOWED_PER_SECOND = 100;

    /**
     * We don't want to issue shard listing requests at the peak allowed rate.
     */
    private static final double RATIO_OF_SHARD_LISTING_RATE_UTILIZED = 0.1;

    /**
     * Failure usually happens due to the over-utilization of resources and/or
     * crossing of various limits. Even if we retry the operation, it is a good
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
        this.knownShards = knownShards.stream().map(Shard::getShardId).collect(toCollection(HashSet::new));
        this.listShardsRateTracker = initRandomizedTracker(totalInstances);
        this.nextListShardsTime = System.nanoTime() + listShardsRateTracker.next();
    }

    public Collection<Shard> probe(long currentTime) {
        if (listShardResult == null) {
            initShardListing(currentTime);
            return emptySet();
        } else {
            return checkForNewShards(currentTime);
        }
    }

    private void initShardListing(long currentTime) {
        if (currentTime < nextListShardsTime) {
            return;
        }
        listShardResult = helper.listShardsAsync(nextToken);
        nextListShardsTime = currentTime + listShardsRateTracker.next();
    }

    private Collection<Shard> checkForNewShards(long currentTime) {
        if (listShardResult.isDone()) {
            try {
                ListShardsResult result = helper.readResult(listShardResult);
                nextToken = result.getNextToken();

                List<Shard> shards = result.getShards();

                Set<Shard> unknownShards = shards.stream()
                        .filter(shard -> shardBelongsToRange(shard, hashRange))
                        .filter(shard -> !knownShards.contains(shard.getShardId())).collect(toCollection(HashSet::new));

                if (!unknownShards.isEmpty()) {
                    logger.info("New shards detected: " +
                            unknownShards.stream().map(Shard::getShardId).collect(joining(", ")));
                    knownShards.addAll(unknownShards.stream().map(Shard::getShardId).collect(toList()));
                }
                return unknownShards;
            } catch (SdkClientException e) {
                logger.warning("Failed listing shards, retrying. Cause: " + e.getMessage());
                nextToken = null;
                nextListShardsTime = currentTime + PAUSE_AFTER_FAILURE;
                return emptySet();
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                listShardResult = null;
            }
        } else {
            return emptySet();
        }
    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int totalInstances) {
        // The maximum rate at which ListStreams operations can be performed on
        // a data stream is 100/second and we need to enforce this, even while
        // we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toNanos(1) * totalInstances,
                (int) (SHARD_LISTINGS_ALLOWED_PER_SECOND * RATIO_OF_SHARD_LISTING_RATE_UTILIZED));
    }

}
