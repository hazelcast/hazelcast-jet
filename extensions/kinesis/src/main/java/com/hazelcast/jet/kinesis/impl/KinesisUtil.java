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
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.hazelcast.jet.JetException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class KinesisUtil {

    private static final int SLEEP_DURATION = 250;

    private KinesisUtil() {
    }

    public static String getStreamStatus(AmazonKinesisAsync kinesis, String stream) {
        DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
        request.setStreamName(stream);

        StreamDescriptionSummary description = kinesis.describeStreamSummary(request).getStreamDescriptionSummary();
        return description.getStreamStatus();
    }

    public static void waitForStreamToActivate(AmazonKinesisAsync kinesis, String stream) {
        while (true) {
            String status = getStreamStatus(kinesis, stream);
            if (StreamStatus.ACTIVE.toString().equals(status)) {
                return;
            } else if (StreamStatus.CREATING.toString().equals(status) ||
                    StreamStatus.UPDATING.toString().equals(status)) {
                waitABit();
            } else if (StreamStatus.DELETING.toString().equals(status)) {
                throw new JetException("Stream is being deleted");
            } else {
                throw new RuntimeException("Programming error, unhandled stream status: " + status);
            }
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

    public static List<Shard> getActiveShards(AmazonKinesisAsync kinesis, String stream) {
        return getActiveShards(kinesis, stream, shard -> true, Function.identity());
    }

    public static <T> List<T> getActiveShards(AmazonKinesisAsync kinesis,
                                              String stream,
                                              Function<? super Shard, T> mapper) {
        return getActiveShards(kinesis, stream, shard -> true, mapper);
    }

    public static List<Shard> getActiveShards(AmazonKinesisAsync kinesis, String stream, Predicate<? super Shard> filter) {
        return getActiveShards(kinesis, stream, filter, Function.identity());
    }

    private static <T> List<T> getActiveShards(
            AmazonKinesisAsync kinesis,
            String stream,
            Predicate<? super Shard> filter,
            Function<? super Shard, T> mapper
    ) {
        List<T> shards = new ArrayList<>();
        String nextToken = null;
        do {
            ListShardsRequest request = KinesisUtil.requestOfShards(stream, nextToken);
            ListShardsResult response = kinesis.listShards(request);
            shards.addAll(response.getShards().stream()
                    .filter(filter)
                    .filter(KinesisUtil::shardActive)
                    .map(mapper)
                    .collect(Collectors.toList()));
            nextToken = response.getNextToken();
        } while (nextToken != null);
        return shards;
    }

    public static ListShardsRequest requestOfShards(@Nonnull String stream, @Nullable String nextToken) {
        ListShardsRequest request = new ListShardsRequest();
        if (nextToken == null) {
            request.setStreamName(stream);
        } else {
            request.setNextToken(nextToken);
        }
        request.setShardFilter(new ShardFilter().withType(ShardFilterType.AT_LATEST));
        return request;
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

    public static String toString(Collection<? extends Shard> shards) { //todo: remove?
        return toString(shards, shard -> true);
    }

    public static String toString(Collection<? extends Shard> shards, Predicate<? super Shard> predicate) {
        return shards.stream()
                .filter(predicate)
                .map(KinesisUtil::toString).collect(Collectors.joining(", "));
    }

    public static String toString(Shard shard) {
        return shard.getShardId();
    }
}
