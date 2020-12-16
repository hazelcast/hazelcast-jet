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

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class KinesisSourcePSupplier implements ProcessorSupplier {

    private static final long serialVersionUID = 1L;

    /**
     * We don't want to create an AWS client for each processor instance,
     * because they aren't light. We also can't do the other extreme, have a
     * single AWS client shared by all processors, because there would be a lot
     * of contention, causing problems. So we use shared clients but use them
     * for a limited number of processor instances, specified by this constant.
     */
    private static final int PROCESSORS_PER_CLIENT = 12;

    @Nonnull
    private final AwsConfig awsConfig;
    @Nonnull
    private final String stream;
    @Nonnull
    private final EventTimePolicy<? super Map.Entry<String, byte[]>> eventTimePolicy;
    @Nonnull
    private final HashRange hashRange;
    @Nonnull
    private final RetryStrategy retryStrategy;

    private transient int memberCount;
    private transient ILogger logger;
    private transient AmazonKinesisAsync[] clients;

    public KinesisSourcePSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super Map.Entry<String, byte[]>> eventTimePolicy,
            @Nonnull HashRange hashRange,
            @Nonnull RetryStrategy retryStrategy
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.eventTimePolicy = eventTimePolicy;
        this.hashRange = hashRange;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void init(@Nonnull Context context) {
        memberCount = context.memberCount();
        logger = context.logger();

        clients = IntStream.range(0, (int) Math.ceil((double) context.localParallelism() / PROCESSORS_PER_CLIENT))
                .mapToObj(IGNORED -> awsConfig.buildClient())
                .toArray(AmazonKinesisAsync[]::new);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        HashRange[] rangePartitions = rangePartitions(count);
        ShardQueue[] shardQueues = shardQueues(count);
        RangeMonitor rangeMonitor = new RangeMonitor(
                memberCount,
                clients[0],
                stream,
                hashRange,
                rangePartitions,
                shardQueues,
                retryStrategy,
                logger
        );

        return IntStream.range(0, count)
                .mapToObj(i ->
                        new KinesisSourceP(
                                clients[i % clients.length],
                                stream,
                                eventTimePolicy,
                                hashRange,
                                rangePartitions[i],
                                shardQueues[i],
                                i == 0 ? rangeMonitor : null,
                                retryStrategy
                        ))
                .collect(toList());
    }

    private ShardQueue[] shardQueues(int count) {
        ShardQueue[] queues = new ShardQueue[count];
        Arrays.setAll(queues, IGNORED -> new ShardQueue());
        return queues;
    }

    private HashRange[] rangePartitions(int count) {
        HashRange[] ranges = new HashRange[count];
        Arrays.setAll(ranges, i -> hashRange.partition(i, count));
        return ranges;
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (clients != null) {
            Arrays.stream(clients).forEach(AmazonKinesis::shutdown);
        }
    }
}
