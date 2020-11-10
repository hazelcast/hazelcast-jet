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
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

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
     * 1 second." */
    private static final long READ_PAUSE_AFTER_THROUGHPUT_EXCEEDED = 1000L;

    /**
     * Maximum number of records returned by this reader in a single batch. Is
     * limited due to being used from a cooperative processor. Should not pose
     * a performance bottleneck, because while available data is being processed
     * the asynchronous request for more will already be issued in the background.
     */
    private static final int DATA_BATCH_SIZE = 100;

    private final Shard shard;
    private final RandomizedRateTracker getRecordsRateTracker =
            new RandomizedRateTracker(1000, GET_RECORD_OPS_PER_SECOND);

    private State state = State.NO_SHARD_ITERATOR;
    private String shardIterator;
    private Future<GetShardIteratorResult> shardIteratorResult;
    private Future<GetRecordsResult> recordsResult;
    private long nextGetRecordsTime;

    private final LinkedList<Record> data = new LinkedList<>();

    ShardReader(AmazonKinesisAsync kinesis, String stream, Shard shard) {
        super(kinesis, stream);
        this.shard = shard;
    }

    public Result run() {
        try {
            switch (state) {
                case NO_SHARD_ITERATOR:
                    shardIteratorResult = kinesis.getShardIteratorAsync(
                            stream,
                            shard.getShardId(),
                            "AT_SEQUENCE_NUMBER",
                            shard.getSequenceNumberRange().getStartingSequenceNumber()
                    ); //todo: proper starting sequence number will be provided from offsets restored from Jet snapshots
                    state = State.WAITING_FOR_SHARD_ITERATOR;
                    return Result.NOTHING;


                case WAITING_FOR_SHARD_ITERATOR:
                    if (shardIteratorResult.isDone()) {
                        shardIterator = shardIteratorResult.get().getShardIterator();
                        state = State.NEED_TO_REQUEST_RECORDS;
                    }
                    return Result.NOTHING;


                case NEED_TO_REQUEST_RECORDS:
                    if (attemptToSendGetRecordsRequest()) {
                        state = State.WAITING_FOR_RECORDS;
                    }

                    return Result.NOTHING;


                case WAITING_FOR_RECORDS:
                    if (recordsResult.isDone()) {
                        try {
                            GetRecordsResult result = recordsResult.get();
                            shardIterator = result.getNextShardIterator();
                            data.addAll(result.getRecords());
                            if (shardIterator == null) {
                                state = State.SHARD_CLOSED;
                                return data.size() > 0 ? Result.HAS_DATA : Result.CLOSED;
                            } else if (data.size() > 0) {
                                state = State.HAS_DATA_NEED_TO_REQUEST_RECORDS;
                                return Result.HAS_DATA;
                            } else {
                                state = State.NEED_TO_REQUEST_RECORDS;
                                return Result.NOTHING;
                            }
                        } catch (ExecutionException e) {
                            Throwable cause = e.getCause();
                            if (cause instanceof ProvisionedThroughputExceededException) {
                                nextGetRecordsTime = System.currentTimeMillis() + READ_PAUSE_AFTER_THROUGHPUT_EXCEEDED;
                                state = State.NEED_TO_REQUEST_RECORDS;
                                return Result.NOTHING;
                            } else {
                                throw rethrow(cause);
                            }
                        }
                    } else {
                        return Result.NOTHING;
                    }


                case HAS_DATA_NEED_TO_REQUEST_RECORDS:
                    if (attemptToSendGetRecordsRequest()) {
                        state = data.size() > 0 ? State.HAS_DATA : State.WAITING_FOR_RECORDS;
                    }

                    return data.size() > 0 ? Result.HAS_DATA : Result.NOTHING;


                case HAS_DATA:
                    state = data.size() > 0 ? State.HAS_DATA : State.WAITING_FOR_RECORDS;
                    return data.size() > 0 ? Result.HAS_DATA : Result.NOTHING;


                case SHARD_CLOSED:
                    return data.size() > 0 ? Result.HAS_DATA : Result.CLOSED;


                default:
                    throw new RuntimeException("Programming error, unhandled state: " + state);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        } catch (ExecutionException e) { //todo: move this to the level of specific operations and handle some cases
            e.printStackTrace();
            throw rethrow(e);
        }
    }

    private boolean attemptToSendGetRecordsRequest() {
        if (System.currentTimeMillis() < nextGetRecordsTime) {
            return false;
        }

        GetRecordsRequest getRecordsRequest = buildGetRecordsRequest(shardIterator);
        recordsResult = kinesis.getRecordsAsync(getRecordsRequest);

        nextGetRecordsTime = System.currentTimeMillis() + getRecordsRateTracker.next();
        return true;
    }

    public Shard getShard() {
        return shard;
    }

    public Record[] getData() {
        if (data.isEmpty()) {
            throw new IllegalStateException("Can't ask for data when none is available");
        }

        Record[] records = new Record[Math.min(data.size(), DATA_BATCH_SIZE)];
        for (int i = 0; i < records.length; i++) {
            records[i] = data.remove();
        }

        return records;
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
         * Running the reader revealed the shard being closed (due to merge or split).
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
         * Has some data read previously, ready to issue request for more.
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

    private static GetRecordsRequest buildGetRecordsRequest(String shardIterator) {
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        return getRecordsRequest;
    }

}
