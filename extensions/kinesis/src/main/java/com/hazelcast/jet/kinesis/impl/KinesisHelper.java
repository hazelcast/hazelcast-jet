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
import com.amazonaws.services.kinesis.model.AmazonKinesisException;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.ExpiredNextTokenException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.hazelcast.jet.JetException;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class KinesisHelper {

    private static final int SLEEP_DURATION = 250;

    private final AmazonKinesisAsync kinesis;
    private final String stream;

    private final ILogger logger;

    public KinesisHelper(AmazonKinesisAsync kinesis, String stream, ILogger logger) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.logger = logger;
    }

    public static boolean shardActive(@Nonnull Shard shard) {
        String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
        return endingSequenceNumber == null;
        //need to rely on this hack, because shard filters don't seem to work, on the mock at least ...
    }

    public static boolean shardBelongsToRange(@Nonnull Shard shard, @Nonnull HashRange range) {
        String startingHashKey = shard.getHashKeyRange().getStartingHashKey();
        return range.contains(new BigInteger(startingHashKey));
    }

    public void waitForStreamToActivate() {
        while (true) {
            StreamStatus status = callSafely(this::getStreamStatus);
            switch (status) {
                case ACTIVE:
                    return;
                case CREATING:
                case UPDATING:
                    logger.info("Waiting for stream " + stream + " to become active...");
                    waitABit();
                    break;
                case DELETING:
                    throw new JetException("Stream is being deleted");
                default:
                    throw new JetException("Programming error, unhandled stream status: " + status);
            }
        }
    }

    public void waitForStreamToDisappear() {
        while (true) {
            List<String> streams = callSafely(this::listStreams);
            if (streams.isEmpty()) {
                return;
            } else {
                logger.info("Waiting for stream " + stream + " to disappear...");
                waitABit();
            }
        }
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

    public List<Shard> listShards(Predicate<? super Shard> filter) {
        return callSafely(this::listShards).stream()
                .filter(filter)
                .collect(Collectors.toList());
    }

    private List<Shard> listShards() throws AmazonKinesisException {
        List<Shard> shards = new ArrayList<>();
        String nextToken = null;
        do {
            ListShardsRequest request = listShardsRequest(nextToken);
            ListShardsResult response = kinesis.listShards(request);
            shards.addAll(response.getShards());
            nextToken = response.getNextToken();
        } while (nextToken != null);
        return shards;
    }

    private ListShardsRequest listShardsRequest(@Nullable String nextToken) {
        ListShardsRequest request = new ListShardsRequest();
        if (nextToken == null) {
            request.setStreamName(stream);
        } else {
            request.setNextToken(nextToken);
        }
        request.setShardFilter(new ShardFilter().withType(ShardFilterType.AT_LATEST));
        return request;
    }

    public Future<ListShardsResult> listShardsAsync(String nextToken) {
        ListShardsRequest request = listShardsRequest(nextToken);
        return kinesis.listShardsAsync(request);
    }

    public Future<GetShardIteratorResult> getShardIteratorAsync(Shard shard, String lastSeenSeqNo) {
        String shardId = shard.getShardId();
        if (lastSeenSeqNo == null) {
            return kinesis.getShardIteratorAsync(stream, shardId, "AT_SEQUENCE_NUMBER",
                    shard.getSequenceNumberRange().getStartingSequenceNumber());
        } else {
            return kinesis.getShardIteratorAsync(stream, shardId, "AFTER_SEQUENCE_NUMBER", lastSeenSeqNo);
        }
    }

    public Future<GetRecordsResult> getRecordsAsync(String shardIterator) {
        GetRecordsRequest request = new GetRecordsRequest();
        request.setShardIterator(shardIterator);
        return kinesis.getRecordsAsync(request);
    }

    public Future<PutRecordsResult> putRecordsAsync(Collection<PutRecordsRequestEntry> entries) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setRecords(entries);
        request.setStreamName(stream);
        return kinesis.putRecordsAsync(request);
    }

    private <T> T callSafely(Callable<T> callable) {
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

            waitABit();
        }
    }

    public <T> T readResult(Future<T> future) throws Throwable {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Interrupted while waiting for results");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    private static void waitABit() {
        //todo: use exponential backup
        try {
            TimeUnit.MILLISECONDS.sleep(SLEEP_DURATION);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Waiting for stream to activate interrupted");
        }
    }
}
