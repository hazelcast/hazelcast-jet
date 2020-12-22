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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.kinesis.impl.KinesisHelper.shardBelongsToRange;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public class RangeMonitor extends AbstractShardWorker {

    /**
     * ListStreams operations are limited to 100 per second, per data stream
     */
    private static final int SHARD_LISTINGS_ALLOWED_PER_SECOND = 100;

    /**
     * We don't want to issue shard listing requests at the peak allowed rate.
     */
    private static final double RATIO_OF_SHARD_LISTING_RATE_UTILIZED = 0.1;

    private final Map<String, Integer> knownShards = new HashMap<>();
    private final HashRange memberHashRange;
    private final HashRange[] rangePartitions;
    private final ShardQueue[] shardQueues;
    private final RandomizedRateTracker listShardsRateTracker;
    private final RetryTracker listShardRetryTracker;

    private String nextToken;
    private Set<String> shardsFromPreviousListing = new HashSet<>();
    private Set<String> shardsFromCurrentListing = new HashSet<>();
    private Future<ListShardsResult> listShardsResult;
    private long nextListShardsTime;

    public RangeMonitor(
            int totalInstances,
            AmazonKinesisAsync kinesis,
            String stream,
            HashRange memberHashRange,
            HashRange[] rangePartitions,
            ShardQueue[] shardQueues,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(kinesis, stream, logger);
        this.memberHashRange = memberHashRange;
        this.rangePartitions = rangePartitions;
        this.shardQueues = shardQueues;
        this.listShardRetryTracker = new RetryTracker(retryStrategy);
        this.listShardsRateTracker = initRandomizedTracker(totalInstances);
        this.nextListShardsTime = System.nanoTime() + listShardsRateTracker.next();
    }

    public void run() {
        if (listShardsResult == null) {
            initShardListing();
        } else {
            checkForNewShards();
        }
    }

    public void addKnownShard(String shardId, BigInteger startingHashKey) {
        knownShards.put(shardId, findOwner(startingHashKey));
    }

    private void initShardListing() {
        long currentTime = System.nanoTime();
        if (currentTime < nextListShardsTime) {
            return;
        }
        listShardsResult = helper.listShardsAsync(nextToken);
        nextListShardsTime = currentTime + listShardsRateTracker.next();
    }

    private void checkForNewShards() {
        if (listShardsResult.isDone()) {
            ListShardsResult result;
            try {
                result = helper.readResult(listShardsResult);
            } catch (SdkClientException e) {
                dealWithListShardsFailure(e);
                return;

                //todo: catch LimitExceededException and handle it differently
                //todo: check against /amazon/kinesis/leases/KinesisShardDetector.java#L188
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                listShardsResult = null;
            }

            listShardRetryTracker.reset();

            Set<Shard> shards = result.getShards().stream()
                    .filter(shard -> shardBelongsToRange(shard, memberHashRange))
                    .collect(Collectors.toSet());
            shardsFromCurrentListing.addAll(shards.stream().map(Shard::getShardId).collect(Collectors.toSet()));

            Set<Shard> newShards = shards.stream()
                    .filter(shard -> !shardsFromPreviousListing.contains(shard.getShardId()))
                    .collect(Collectors.toSet());

            if (!newShards.isEmpty()) {
                logger.info("New shards detected: " +
                        newShards.stream().map(Shard::getShardId).collect(joining(", ")));

                for (Shard shard : newShards) {
                    int index = findOwner(new BigInteger(shard.getHashKeyRange().getStartingHashKey()));
                    knownShards.put(shard.getShardId(), index);
                    shardQueues[index].added(shard);
                }
            }

            nextToken = result.getNextToken();
            if (nextToken == null) {
                Set<Map.Entry<String, Integer>> expiredShards = knownShards.entrySet().stream()
                        .filter(e -> !shardsFromCurrentListing.contains(e.getKey()))
                        .collect(Collectors.toSet());
                for (Map.Entry<String, Integer> e : expiredShards) {
                    String shardId = e.getKey();
                    logger.info("Expired shard detected: " + shardId);
                    knownShards.remove(shardId);
                    shardQueues[e.getValue()].expired(shardId);
                }

                Set<String> tmp = shardsFromPreviousListing;
                tmp.clear();

                shardsFromPreviousListing = shardsFromCurrentListing;

                shardsFromCurrentListing = tmp;
            }
        }
    }

    private void dealWithListShardsFailure(@Nonnull Exception failure) {
        nextToken = null;
        shardsFromCurrentListing.clear();

        listShardRetryTracker.attemptFailed();
        if (listShardRetryTracker.shouldTryAgain()) {
            long timeoutMillis = listShardRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed listing shards, retrying in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextListShardsTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
        } else {
            throw rethrow(failure);
        }

    }

    private int findOwner(BigInteger startingHashKey) {
        for (int i = 0; i < rangePartitions.length; i++) { //todo: could do binary search, but is it worth it?
            HashRange range = rangePartitions[i];
            if (KinesisHelper.shardBelongsToRange(startingHashKey, range)) {
                return i;
            }
        }
        throw new JetException("Programming error, shard not covered by any hash range");
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
