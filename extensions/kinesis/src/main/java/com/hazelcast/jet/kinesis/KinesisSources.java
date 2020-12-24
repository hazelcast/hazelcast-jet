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

import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.KinesisSourcePMetaSupplier;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Contains factory methods for creating Amazon Kinesis Data Streams
 * (KDS) sources.
 *
 * @since 4.4
 */
public final class KinesisSources {

    private KinesisSources() {
    }

    //todo: describe exposed metrics

    /**
     * Initiates the building of a streaming source which consumes a
     * Kinesis data stream and emits {@code Map.Entry<String, byte[]>}
     * items.
     * <p>
     * Each emitted item represents a Kinesis <em>record</em>, the basic
     * unit of data stored in KDS. A record is composed of a sequence
     * number, partition key, and data blob. This source ignores the
     * sequence numbers.
     * <p>
     * The <em>partition key</em> is used to segregate and route records
     * in Kinesis and is specified by the data producer, while adding
     * data to KDS. The keys of the returned items are the record
     * partition keys, unicode strings with a maximum length of 256
     * characters.
     * <p>
     * The <em>data blob</em> of the record is returned as the values of
     * the {@code Map.Entry} items, in the form of {@code byte} arrays.
     * The maximum size of the data blob is 1 MB.
     * <p>
     * The source is <em>distributed</em>, each instance is consuming
     * data from zero, one or more Kinesis shards, the base throughput
     * units of KDS. One shard provides a capacity of 2MB/sec data
     * output. Items with the same partition key always come from the
     * same shard, one shard contains multiple partition keys. Items
     * coming from the same shard are ordered and the source preserves
     * this order (except on resharding). The local parallelism of the
     * source is not defined, so will depend on the number of cores
     * available.
     * <p>
     * If snapshotting is enabled shard offsets are saved to the
     * snapshßot. After a restart, the source emits the events starting
     * from the same offset. The source supports both
     * <em>at-least-once</em> and <em>exactly-once</em> processing
     * guarantees.
     * <p>
     * The source is able to provide native timestams, in the sense that
     * they are read from KDS, but be aware that they are actually
     * Kinesis ingestion times, not event times in the strictest sense.
     * <p>
     * As stated before the source preserves the ordering inside shards.
     * However, Kinesis supports resharding, which lets you adjust the
     * number of shards in your stream to adapt to changes in data flow
     * rate through the stream. When resharding happens the source is
     * not able to preserve the order among the last items of a shard
     * being destroyed and the first items of new shards being created.
     *
     * @param stream name of the Kinesis stream being consumed by the
     * source
     * @return fluent builder that can be used to set properties and
     * also to construct the source once configuration is done
     */
    @Nonnull
    public static Builder kinesis(@Nonnull String stream) {
        return new Builder(Objects.requireNonNull(stream));
    }

    /**
     * Fluent builder for constructing the Kinesis source and setting
     * its configuration parameters.
     */
    public static final class Builder {

        private static final long INITIAL_RETRY_TIMEOUT_MS = 100L;
        private static final double EXPONENTIAL_BACKOFF_MULTIPLIER = 2.0;
        private static final long MAXIMUM_RETRY_TIMEOUT_MS = 3_000L;

        private static final RetryStrategy DEFAULT_RETRY_STRATEGY = RetryStrategies.custom()
                .intervalFunction(IntervalFunction.exponentialBackoffWithCap(
                        INITIAL_RETRY_TIMEOUT_MS, EXPONENTIAL_BACKOFF_MULTIPLIER, MAXIMUM_RETRY_TIMEOUT_MS))
                .build();

        @Nonnull
        private final String stream;
        @Nonnull
        private final AwsConfig config = new AwsConfig();
        @Nonnull
        private RetryStrategy retryStrategy = DEFAULT_RETRY_STRATEGY;

        private Builder(@Nonnull String stream) {
            this.stream = stream;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withEndpoint(@Nullable String endpoint) {
            config.setEndpoint(endpoint);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withRegion(@Nullable String region) {
            config.setRegion(region);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            config.setCredentials(accessKey, secretKey);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withRetryStrategy(@Nonnull RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public StreamSource<Map.Entry<String, byte[]>> build() {
            String stream = this.stream;
            AwsConfig config = this.config;
            RetryStrategy retryStrategy = this.retryStrategy;
            return Sources.streamFromProcessorWithWatermarks(
                    "Kinesis Source (" + stream + ")",
                    true,
                    eventTimePolicy -> new KinesisSourcePMetaSupplier(config, stream, retryStrategy, eventTimePolicy));
        }
    }

}
