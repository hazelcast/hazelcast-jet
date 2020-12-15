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
package com.hazelcast.jet.kinesis;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.ExpiredNextTokenException;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.kinesis.impl.KinesisHelper;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class KinesisTestHelper {

    static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
            .maxAttempts(10)
            .intervalFunction(IntervalFunction.exponentialBackoffWithCap(250L, 2.0, 1000L))
            .build();

    private final AmazonKinesisAsync kinesis;
    private final String stream;

    private final ILogger logger;

    KinesisTestHelper(AmazonKinesisAsync kinesis, String stream, ILogger logger) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.logger = logger;
    }

    public boolean streamExists() {
        Set<String> streams = new HashSet<>(callSafely(this::listStreams));
        return streams.contains(stream);
    }

    public void waitForStreamToActivate() {
        int attempt = 0;
        while (true) {
            StreamStatus status = callSafely(this::getStreamStatus);
            switch (status) {
                case ACTIVE:
                    return;
                case CREATING:
                case UPDATING:
                    logger.info("Waiting for stream " + stream + " to become active...");
                    wait(++attempt);
                    break;
                case DELETING:
                    throw new JetException("Stream is being deleted");
                default:
                    throw new JetException("Programming error, unhandled stream status: " + status);
            }
        }
    }

    public void waitForStreamToDisappear() {
        int attempt = 0;
        while (true) {
            List<String> streams = callSafely(this::listStreams);
            if (streams.contains(stream)) {
                logger.info("Waiting for stream " + stream + " to disappear...");
                wait(++attempt);
            } else {
                return;
            }
        }
    }

    public void createStream(int shardCount) {
        if (streamExists()) {
            throw new IllegalStateException("Stream already exists");
        }

        callSafely(() -> {
            CreateStreamRequest request = new CreateStreamRequest();
            request.setShardCount(shardCount);
            request.setStreamName(stream);
            return kinesis.createStream(request);
        });

        waitForStreamToActivate();
    }

    public void deleteStream() {
        if (streamExists()) {
            callSafely(() -> kinesis.deleteStream(stream));
            waitForStreamToDisappear();
        }
    }

    public List<Shard> listShards(Predicate<? super Shard> filter) {
        return callSafely(this::listShards).stream()
                .filter(filter)
                .collect(Collectors.toList());
    }

    private List<String> listStreams() {
        return kinesis.listStreams().getStreamNames();
    }

    private StreamStatus getStreamStatus() {
        DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
        request.setStreamName(stream);

        StreamDescriptionSummary description = kinesis.describeStreamSummary(request).getStreamDescriptionSummary();
        String statusString = description.getStreamStatus();

        return StreamStatus.valueOf(statusString);
    }

    private List<Shard> listShards() {
        List<Shard> shards = new ArrayList<>();
        String nextToken = null;
        do {
            ListShardsRequest request = KinesisHelper.listShardsRequest(stream, nextToken);
            ListShardsResult response = kinesis.listShards(request);
            shards.addAll(response.getShards());
            nextToken = response.getNextToken();
        } while (nextToken != null);
        return shards;
    }

    private <T> T callSafely(Callable<T> callable) {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (LimitExceededException lee) {
                String message = "The requested resource exceeds the maximum number allowed, or the number of " +
                        "concurrent stream requests exceeds the maximum number allowed. Will retry.";
                logger.warning(message, lee);
            } catch (ExpiredNextTokenException ente) {
                String message = "The pagination token passed to the operation is expired. Will retry.";
                logger.warning(message, ente);
            } catch (ResourceInUseException riue) {
                String message = "The resource is not available for this operation. For successful operation, the " +
                        "resource must be in the ACTIVE state. Will retry.";
                logger.warning(message, riue);
            } catch (ResourceNotFoundException rnfe) {
                String message = "The requested resource could not be found. The stream might not be specified correctly.";
                throw new JetException(message, rnfe);
            } catch (InvalidArgumentException iae) {
                String message = "A specified parameter exceeds its restrictions, is not supported, or can't be used.";
                throw new JetException(message, iae);
            } catch (SdkClientException sce) {
                String message = "Amazon SDK failure, ignoring and retrying.";
                logger.warning(message, sce);
            } catch (Exception e) {
                throw rethrow(e);
            }

            wait(++attempt);
        }
    }

    private static void wait(int attempt) {
        long duration = RETRY_STRATEGY.getIntervalFunction().waitAfterAttempt(attempt);
        try {
            TimeUnit.MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Waiting for stream to activate interrupted");
        }
    }

}
