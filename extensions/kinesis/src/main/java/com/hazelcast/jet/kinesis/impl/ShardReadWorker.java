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
import com.amazonaws.services.kinesis.model.Record;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class ShardReadWorker implements ShardWorker {

    //todo: shard iterators expire after 5 minutes... how do we deal with that?

    //todo: getting a shard iterator can be done only 5 times per second per shard; we need to make sure

    //todo: now we read every record from the shard; we'll probably want to do that only starting from
    // a certain sequence number, once we have snapshotted offsets

    //todo: Each data record can be up to 1 MiB in size, and each shard can read up to 2 MiB per second.
    // You can ensure that your calls don't exceed the maximum supported size or throughput by using the
    // Limit parameter to specify the maximum number of records that GetRecords can return. Consider your
    // average record size when determining this limit. The maximum number of records that can be returned
    // per call is 10,000.
    // The size of the data returned by GetRecords varies depending on the utilization of the shard.
    // The maximum size of data that GetRecords can return is 10 MiB. If a call returns this amount of data,
    // subsequent calls made within the next 5 seconds throw ProvisionedThroughputExceededException. If there
    // is insufficient provisioned throughput on the stream, subsequent calls made within the next 1 second
    // throw ProvisionedThroughputExceededException. GetRecords doesn't return any data when it throws an exception.
    // For this reason, we recommend that you wait 1 second between calls to GetRecords. However, it's possible
    // that the application will get exceptions for longer than 1 second.

    private final AmazonKinesisAsync kinesis;
    private final String stream;
    private final String shardId;

    private State state = State.NO_SHARD_ITERATOR;

    private String shardIterator;
    private Future<GetShardIteratorResult> shardIteratorResult;
    private Future<GetRecordsResult> recordsResult;

    ShardReadWorker(AmazonKinesisAsync kinesis, String stream, String shardId) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.shardId = shardId;
    }

    @Override
    @Nonnull
    public List<Record> poll() {
        try {
            switch (state) {
                case NO_SHARD_ITERATOR:
                    shardIteratorResult = kinesis.getShardIteratorAsync(stream, shardId, "TRIM_HORIZON");
                    state = State.WAITING_FOR_SHARD_ITERATOR;
                    return Collections.emptyList();
                case WAITING_FOR_SHARD_ITERATOR:
                    if (shardIteratorResult.isDone()) {
                        shardIterator = shardIteratorResult.get().getShardIterator();
                        state = State.READY_TO_READ_RECORDS;
                    }
                    return Collections.emptyList();
                case READY_TO_READ_RECORDS:
                    GetRecordsRequest getRecordsRequest = buildGetRecordsRequest(shardIterator);
                    recordsResult = kinesis.getRecordsAsync(getRecordsRequest);
                    state = State.WAITING_FOR_RECORDS;
                    return Collections.emptyList();
                case WAITING_FOR_RECORDS:
                    if (recordsResult.isDone()) {
                        GetRecordsResult recordsResult = this.recordsResult.get();
                        List<Record> records = recordsResult.getRecords();

                        shardIterator = recordsResult.getNextShardIterator();
                        if (shardIterator == null) {
                            state = State.SHARD_CLOSED;
                        } else {
                            state = State.READY_TO_READ_RECORDS;
                        }

                        return records;
                    } else {
                        return Collections.emptyList();
                    }
                case SHARD_CLOSED:
                    throw new RuntimeException(); //todo: handle splits & merges
                default:
                    throw new RuntimeException(); //todo
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        } catch (ExecutionException e) {
            throw rethrow(e);
        }
    }

    private static GetRecordsRequest buildGetRecordsRequest(String shardIterator) {
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        return getRecordsRequest;
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
         * Records ready to be read.
         */
        READY_TO_READ_RECORDS,

        /**
         * Reading records initiated, waiting for results.
         */
        WAITING_FOR_RECORDS,

        /**
         * Shard has been terminated, due to a split or a merge.
         */
        SHARD_CLOSED,
        ;
    }

}
