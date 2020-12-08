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
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ShardReader extends AbstractShardWorker {

    /* Kinesis allows for a maximum of 5 GetRecords operations per second. */
    private static final int GET_RECORD_OPS_PER_SECOND = 5;

    private final Shard shard;
    private final RandomizedRateTracker getRecordsRateTracker =
            new RandomizedRateTracker(1000, GET_RECORD_OPS_PER_SECOND);

    private State state = State.NO_SHARD_ITERATOR;
    private String shardIterator;

    private Future<GetShardIteratorResult> shardIteratorResult;
    private final RetryTracker getShardIteratorRetryTracker;
    private long nextGetShardIteratorTime = System.nanoTime();

    private Future<GetRecordsResult> recordsResult;
    private final RetryTracker readRecordRetryTracker;
    private long nextGetRecordsTime = System.nanoTime();

    private List<Record> data = Collections.emptyList();
    private String lastSeenSeqNo;

    ShardReader(
            AmazonKinesisAsync kinesis,
            String stream,
            Shard shard,
            String lastSeenSeqNo,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(kinesis, stream, logger);
        this.shard = shard;
        this.lastSeenSeqNo = lastSeenSeqNo;
        this.readRecordRetryTracker = new RetryTracker(retryStrategy);
        this.getShardIteratorRetryTracker = new RetryTracker(retryStrategy);
    }

    public Result probe(long currentTime) {
        switch (state) {
            case NO_SHARD_ITERATOR:
                return handleNoShardIterator(currentTime);
            case WAITING_FOR_SHARD_ITERATOR:
                return handleWaitingForShardIterator(currentTime);
            case NEED_TO_REQUEST_RECORDS:
                return handleNeedToRequestRecords(currentTime);
            case WAITING_FOR_RECORDS:
                return handleWaitingForRecords(currentTime);
            case HAS_DATA_NEED_TO_REQUEST_RECORDS:
                return handleHasDataNeedToRequestRecords(currentTime);
            case HAS_DATA:
                return handleHasData();
            case SHARD_CLOSED:
                return handleShardClosed();
            default:
                throw new RuntimeException("Programming error, unhandled state: " + state);
        }
    }

    private Result handleNoShardIterator(long currentTime) {
        if (attemptToSendGetShardIteratorRequest(currentTime)) {
            state = State.WAITING_FOR_SHARD_ITERATOR;
        }
        return Result.NOTHING;
    }

    private boolean attemptToSendGetShardIteratorRequest(long currentTime) {
        if (currentTime < nextGetShardIteratorTime) {
            return false;
        }
        shardIteratorResult = helper.getShardIteratorAsync(shard, lastSeenSeqNo);
        nextGetShardIteratorTime = currentTime;
        return true;
    }

    private Result handleWaitingForShardIterator(long currentTime) {
        if (shardIteratorResult.isDone()) {
            try {
                shardIterator = helper.readResult(shardIteratorResult).getShardIterator();
            } catch (SdkClientException sce) {
                return dealWithGetShardIteratorFailure(currentTime,
                        "Failed retrieving shard iterator, retrying in %d ms.Cause: " + sce.getMessage());
            } catch (Throwable t) {
                throw rethrow(t);
            }

            getShardIteratorRetryTracker.reset();

            state = State.NEED_TO_REQUEST_RECORDS;
        }
        return Result.NOTHING;
    }

    @Nonnull
    private Result dealWithGetShardIteratorFailure(long currentTime, String message) {
        getShardIteratorRetryTracker.attemptFailed();
        long timeoutMillis = getShardIteratorRetryTracker.getNextWaitTimeMillis();
        logger.warning(String.format(message, timeoutMillis));
        nextGetShardIteratorTime = currentTime + MILLISECONDS.toNanos(timeoutMillis);
        state = State.NO_SHARD_ITERATOR;
        return Result.NOTHING;
    }

    private Result handleNeedToRequestRecords(long currentTime) {
        if (attemptToSendGetRecordsRequest(currentTime)) {
            state = State.WAITING_FOR_RECORDS;
        }

        return Result.NOTHING;
    }

    private Result handleWaitingForRecords(long currentTime) {
        if (recordsResult.isDone()) {
            GetRecordsResult result;
            try {
                result = helper.readResult(recordsResult);
            } catch (ProvisionedThroughputExceededException pte) {
                return dealWithReadRecordFailure(currentTime, "Data throughput rate exceeded. Backing off and retrying " +
                        "in %d ms.");
            } catch (ExpiredIteratorException eie) {
                return dealWithReadRecordFailure(currentTime, "Record iterator expired. Retrying in %d ms.");
            } catch (SdkClientException sce) {
                return dealWithReadRecordFailure(currentTime,
                        "Failed reading records, retrying in %d. Cause: " + sce.getMessage());
            } catch (Throwable t) {
                throw rethrow(t);
            }

            readRecordRetryTracker.reset();

            shardIterator = result.getNextShardIterator();
            data = result.getRecords();
            if (!data.isEmpty()) {
                lastSeenSeqNo = data.get(data.size() - 1).getSequenceNumber();
            }
            if (shardIterator == null) {
                state = State.SHARD_CLOSED;
                return data.isEmpty() ? Result.CLOSED : Result.HAS_DATA;
            } else if (!data.isEmpty()) {
                state = State.HAS_DATA_NEED_TO_REQUEST_RECORDS;
                return Result.HAS_DATA;
            } else {
                state = State.NEED_TO_REQUEST_RECORDS;
                return Result.NOTHING;
            }
        } else {
            return Result.NOTHING;
        }
    }

    private Result dealWithReadRecordFailure(long currentTime, String message) {
        readRecordRetryTracker.attemptFailed();
        long timeoutMillis = readRecordRetryTracker.getNextWaitTimeMillis();
        logger.warning(String.format(message, timeoutMillis));
        nextGetRecordsTime = currentTime + MILLISECONDS.toNanos(timeoutMillis);
        state = State.NEED_TO_REQUEST_RECORDS;
        return Result.NOTHING;
    }

    private Result handleHasDataNeedToRequestRecords(long currentTime) {
        if (attemptToSendGetRecordsRequest(currentTime)) {
            state = data.isEmpty() ? State.WAITING_FOR_RECORDS : State.HAS_DATA;
        }

        return data.isEmpty() ? Result.NOTHING : Result.HAS_DATA;
    }

    private Result handleHasData() {
        state = data.isEmpty() ? State.WAITING_FOR_RECORDS : State.HAS_DATA;
        return data.isEmpty() ? Result.NOTHING : Result.HAS_DATA;
    }

    private Result handleShardClosed() {
        return data.isEmpty() ? Result.CLOSED : Result.HAS_DATA;
    }

    private boolean attemptToSendGetRecordsRequest(long currentTime) {
        if (currentTime < nextGetRecordsTime) {
            return false;
        }
        recordsResult = helper.getRecordsAsync(shardIterator);
        nextGetRecordsTime = currentTime + getRecordsRateTracker.next();
        return true;
    }

    public Shard getShard() {
        return shard;
    }

    public String getLastSeenSeqNo() {
        return lastSeenSeqNo;
    }

    public Traverser<Record> clearData() {
        if (data.isEmpty()) {
            throw new IllegalStateException("Can't ask for data when none is available");
        }

        Traverser<Record> traverser = Traversers.traverseIterable(data);
        data = Collections.emptyList();
        return traverser;
    }

    enum Result {
        /**
         * Running the reader has not produced any events that need handling.
         */
        NOTHING,

        /**
         * Running the reader has produced new data to be processed.
         */
        HAS_DATA,

        /**
         * The shard was read up to its end (due to merge or split).
         */
        CLOSED
    }

    private enum State {
        /**
         * Shard iterator not available and has not been requested.
         */
        NO_SHARD_ITERATOR,

        /**
         * Shard iterator not available but has already been requested.
         */
        WAITING_FOR_SHARD_ITERATOR,

        /**
         * Has no data available, ready to read some.
         */
        NEED_TO_REQUEST_RECORDS,

        /**
         * Has no data, reading records initiated, waiting for results.
         */
        WAITING_FOR_RECORDS,

        /**
         * Has some data read previously, ready to issue a request for more.
         * The previously read data might get processed while more arrives.
         */
        HAS_DATA_NEED_TO_REQUEST_RECORDS,

        /**
         * Has some data read previously, reading more initiated, waiting for
         * the processing of what's already available to finish.
         */
        HAS_DATA,

        /**
         * Shard has been terminated, due to a split or a merge.
         */
        SHARD_CLOSED,
        ;
    }

}
