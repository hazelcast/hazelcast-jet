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
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ShardCountMonitor extends AbstractShardWorker {

    /**
     * DescribeStreamSummary operations are limited to 20 per second, per account.
     */
    private static final int DESCRIBE_STREAM_OPERATIONS_ALLOWED_PER_SECOND = 20;

    /**
     * We don't want to issue describe stream operations at the peak allowed rate.
     */
    private static final double RATIO_OF_DESCRIBE_STREAM_RATE_UTILIZED = 0.1;

    private final AtomicInteger shardCount;
    private final RandomizedRateTracker descriteStreamRateTracker;
    private final RetryTracker describeStreamRetryTracker;

    private Future<DescribeStreamSummaryResult> describeStreamResult;
    private long nextDescribeStreamTime;

    public ShardCountMonitor(
            AtomicInteger shardCount,
            int totalInstances,
            AmazonKinesisAsync kinesis,
            String stream,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(kinesis, stream, logger);
        this.shardCount = shardCount;
        this.describeStreamRetryTracker = new RetryTracker(retryStrategy);
        this.descriteStreamRateTracker = initRandomizedTracker(totalInstances);
        this.nextDescribeStreamTime = System.nanoTime() + descriteStreamRateTracker.next();
    }

    public void run() {
        long currentTime = System.nanoTime();
        if (describeStreamResult == null) {
            initDescribeStream(currentTime);
        } else {
            checkForStreamDescription(currentTime);
        }
    }

    private void initDescribeStream(long currentTime) {
        if (currentTime < nextDescribeStreamTime) {
            return;
        }
        describeStreamResult = helper.describeStreamSummaryAsync();
        nextDescribeStreamTime = currentTime + descriteStreamRateTracker.next();
    }

    private void checkForStreamDescription(long currentTime) {
        if (describeStreamResult.isDone()) {
            DescribeStreamSummaryResult result;
            try {
                result = helper.readResult(describeStreamResult);
            } catch (SdkClientException e) {
                describeStreamRetryTracker.attemptFailed();
                long timeoutMillis = describeStreamRetryTracker.getNextWaitTimeMillis();
                logger.warning("Failed obtaining stream description, retrying in " + timeoutMillis + " ms. Cause:" +
                        " " + e.getMessage());
                nextDescribeStreamTime = currentTime + MILLISECONDS.toNanos(timeoutMillis);
                return;
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                describeStreamResult = null;
            }

            describeStreamRetryTracker.reset();

            int newShardCount = result.getStreamDescriptionSummary().getOpenShardCount();
            int oldShardCount = shardCount.getAndSet(newShardCount);
            if (oldShardCount != newShardCount) {
                logger.info(String.format("Updated shard count for stream %s: %d", stream, newShardCount));
            }
        }
    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int totalInstances) {
        // The maximum rate at which DescribeStreamSummary operations can be
        // performed on a data stream is 20/second and we need to enforce this,
        // even while we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toNanos(1) * totalInstances,
                (int) (DESCRIBE_STREAM_OPERATIONS_ALLOWED_PER_SECOND * RATIO_OF_DESCRIBE_STREAM_RATE_UTILIZED));
    }

}
