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
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;

class ShardReader extends AbstractShardWorker {

    /* Kinesis allows for a maximum of 5 GetRecords operations per second. */
    private static final int GET_RECORD_OPS_PER_SECOND = 5;

    /* Even though GetRecords operations are limited to 5 per second, if one
     * such operation happens to return too much data, following operations will
     * throw ProvisionedThroughputExceededException. In such cases we need to
     * wait a bit longer than for regular rate limiting.
     *
     * Relevant section from AWS documentation:
     *
     * "The size of the data returned by GetRecords varies depending on the
     * utilization of the shard. The maximum size of data that GetRecords can
     * return is 10 MiB. If a call returns this amount of data, subsequent calls
     * made within the next 5 seconds throw
     * ProvisionedThroughputExceededException. If there is insufficient
     * provisioned throughput on the stream, subsequent calls made within the
     * next 1 second throw ProvisionedThroughputExceededException. GetRecords
     * doesn't return any data when it throws an exception. For this reason, we
     * recommend that you wait 1 second between calls to GetRecords. However,
     * it's possible that the application will get exceptions for longer than
     * 1 second."
     *
     * We also need to add this extra wait whenever we encounter other unexpected
     * failures.
     * */
    private static final long PAUSE_AFTER_FAILURE = SECONDS.toNanos(1); //todo: exponential backoff

    private final Shard shard;
    private final RandomizedRateTracker getRecordsRateTracker =
            new RandomizedRateTracker(1000, GET_RECORD_OPS_PER_SECOND);

    private State state = State.NO_SHARD_ITERATOR;
    private String shardIterator;
    private Future<GetShardIteratorResult> shardIteratorResult;
    private Future<GetRecordsResult> recordsResult;
    private long nextGetRecordsTime = System.nanoTime();

    @Nonnull
    private List<Record> data = Collections.emptyList();
    @Nullable
    private String lastSeenSeqNo;

    ShardReader(AmazonKinesisAsync kinesis, String stream, Shard shard, String lastSeenSeqNo, ILogger logger) {
        super(kinesis, stream, logger);
        this.shard = shard;
        this.lastSeenSeqNo = lastSeenSeqNo;
    }

    public Result probe(long currentTime) {
        switch (state) {
            case NO_SHARD_ITERATOR:
                return handleNoShardIterator();
            case WAITING_FOR_SHARD_ITERATOR:
                return handleWaitingForShardIterator();
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

    private Result handleNoShardIterator() {
        shardIteratorResult = helper.getShardIteratorAsync(shard, lastSeenSeqNo);
        state = State.WAITING_FOR_SHARD_ITERATOR;
        return Result.NOTHING;
    }

    private Result handleWaitingForShardIterator() {
        if (shardIteratorResult.isDone()) {
            try {
                shardIterator = helper.readResult(shardIteratorResult).getShardIterator();
                state = State.NEED_TO_REQUEST_RECORDS;
            } catch (SdkClientException sce) {
                logger.warning("Failed retrieving shard iterator, retrying. Cause: " + sce.getMessage());
                state = State.NO_SHARD_ITERATOR;
                return Result.NOTHING;
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
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
            try {
                GetRecordsResult result = helper.readResult(recordsResult);
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
            } catch (ProvisionedThroughputExceededException pte) {
                return dealWithReadRecordFailure(currentTime, "Data throughput rate exceeded. Backing off and retrying.");
            } catch (ExpiredIteratorException eie) {
                return dealWithReadRecordFailure(currentTime, "Record iterator expired. Retrying.");
            } catch (SdkClientException sce) {
                return dealWithReadRecordFailure(currentTime,
                        "Failed reading records, retrying. Cause: " + sce.getMessage());
            } catch (Throwable t) {
                throw rethrow(t);
            }
        } else {
            return Result.NOTHING;
        }
    }

    private Result dealWithReadRecordFailure(long currentTime, String message) {
        logger.warning(message);
        nextGetRecordsTime = currentTime + PAUSE_AFTER_FAILURE;
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
