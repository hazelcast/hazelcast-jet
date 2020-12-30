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

    /**
     * Name of the metric exposed by the source, used to monitor if reading a
     * specific shard is delayed or not.  The value represents the number of
     * milliseconds the last read record is behind the tip of the stream.
     */
    public static final String MILLIS_BEHIND_LATEST_METRIC = "millisBehindLatest";

    private KinesisSources() {
    }

    /**
     * Initiates the building of a streaming source which consumes a
     * Kinesis data stream and emits {@code Map.Entry<String, byte[]>}
     * items.
     * <p>
     * Each emitted item represents a Kinesis <em>record</em>, the basic
     * unit of data stored in KDS. A record is composed of a sequence
     * number, partition key, and data blob. This source does not expose
     * the sequence numbers, uses them only internally, for replay
     * purposes.
     * <p>
     * The <em>partition key</em> is used to group related records and
     * route them inside Kinesis. Partition keys are unicode strings
     * with a maximum length of 256 characters. They are specified by
     * the data producer, while adding data to KDS.
     * <p>
     * The <em>data blob</em> of the record is provided in the form of a
     * {@code byte} array, in essence serialized data (Kinesis doesn't
     * handle serialization internally). The maximum size of the data
     * blob is 1 MB.
     * <p>
     * The source is <em>distributed</em>, each instance is consuming
     * data from zero, one or more Kinesis <em>shards</em>, the base
     * throughput units of KDS. One shard provides a capacity of 2MB/sec
     * data output. Items with the same partition key always come from
     * the same shard, one shard contains multiple partition keys. Items
     * coming from the same shard are ordered and the source preserves
     * this order (except on resharding). The local parallelism of the
     * source is not defined, so will depend on the number of cores
     * available.
     * <p>
     * If a processing guarantee is specified for the job, Jet will
     * periodically save the current shard offsets internally and then
     * replay from the saved offsets when the job is restarted. If no
     * processing guarantee is enabled, the source will start reading
     * from the oldest available data, determined by the KDS retention
     * period (defaults to 24 hours, can be as long as 365 days). This
     * replay capability make the source suitable for pipelines with
     * both <em>at-least-once</em> and <em>exactly-once</em> processing
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
     * <p>
     * The source provides a metric called
     * {@value MILLIS_BEHIND_LATEST_METRIC}, which specifies, for each
     * shard the source is actively reading data from, if there is any
     * delay in reading the data.
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
        private final AwsConfig awsConfig = new AwsConfig();
        @Nonnull
        private RetryStrategy retryStrategy = DEFAULT_RETRY_STRATEGY;

        private Builder(@Nonnull String stream) {
            this.stream = stream;
        }

        /**
         * Specifies the AWS Kinesis endpoint (URL of the entry point
         * for the AWS web service) to connect to. The general syntax of
         * these endpoint URLs is
         * "{@code protocol://service-code.region-code.amazonaws.com}",
         * so for example for Kinesis, for the {@code us-west-2} region
         * we could have
         * "{@code https://dynamodb.us-west-2.amazonaws.com}". For local
         * testing it might be "{@code http://localhost:4566}".
         * <p>
         * If not specified (or specified as {@code null}), the default
         * endpoint for the specified region will be used.
         */
        @Nonnull
        public Builder withEndpoint(@Nullable String endpoint) {
            awsConfig.withEndpoint(endpoint);
            return this;
        }

        /**
         * Specifies the AWS Region (collection of AWS resources in a
         * geographic area) to connect to. Region names are of form
         * "{@code us-west-1}", "{@code eu-central-1}" and so on.
         * <p>
         * If not specified (or specified as {@code null}), the default
         * region set via external means will be used (either from you
         * local {@code .aws/config} file, or from the
         * {@code AWS_REGION} environment variable). If no such default
         * is set, then "{@code us-east-1}" will be used.
         */
        @Nonnull
        public Builder withRegion(@Nullable String region) {
            awsConfig.withRegion(region);
            return this;
        }

        /**
         * Specifies the AWS credentials to use for authentication
         * purposes.
         * <p>
         * If not specified (or specified as {@code null}), then keys
         * specified via external means will be used. This can mean the
         * local {@code .aws/credentials} file, or the
         * {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}
         * environmental variables.
         * <p>
         * Either both key must be set to non {@code null} values or
         * neither.
         */
        @Nonnull
        public Builder withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            awsConfig.withCredentials(accessKey, secretKey);
            return this;
        }

        /**
         * Specifies how the source should behave when reading data from
         * the stream fails. The default behaviour is that it retries
         * the read operation indefinitely, but after an exponentially
         * increasing delay; starts with 100 milliseconds and doubles on
         * each subsequent failure. A successful read resets it. The
         * delay is capped at 3 seconds.
         */
        @Nonnull
        public Builder withRetryStrategy(@Nonnull RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }

        /**
         * Constructs the source based on the options provided so far.
         */
        @Nonnull
        public StreamSource<Map.Entry<String, byte[]>> build() {
            String stream = this.stream;
            AwsConfig awsConfig = this.awsConfig;
            RetryStrategy retryStrategy = this.retryStrategy;
            return Sources.streamFromProcessorWithWatermarks(
                    "Kinesis Source (" + stream + ")",
                    true,
                    eventTimePolicy -> new KinesisSourcePMetaSupplier(awsConfig, stream, retryStrategy, eventTimePolicy));
        }
    }

}
