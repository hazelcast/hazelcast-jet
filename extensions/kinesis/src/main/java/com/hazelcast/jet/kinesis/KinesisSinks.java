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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.KinesisSinkPSupplier;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.DISTRIBUTED_PARTITIONED;

/**
 * todo: javadoc
 *
 * @since 4.4
 */
public final class KinesisSinks {

    /**
     * Kinesis partition keys are Unicode strings, with a maximum length limit
     * of 256 characters for each key.
     */
    public static final int MAXIMUM_KEY_LENGTH = 256;

    /**
     * The length of a record's data blob (byte array length), plus the record's
     * key size (no. of unicode characters in the key) must not be larger than
     * 1M.
     */
    public static final int MAX_RECORD_SIZE = 1024 * 1024;

    private KinesisSinks() {
    }

    //todo: describe exposed metrics

    /**
     * todo: javadoc
     */
    @Nonnull
    public static <T> Builder<T> kinesis(
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn
    ) {
        return new Builder<>(stream, keyFn, valueFn);
    }

    /**
     * todo: javadoc
     */
    @Nonnull
    public static Builder<Map.Entry<String, byte[]>> kinesis(@Nonnull String stream) {
        return new Builder<>(stream, Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * todo: javadoc
     * @param <T>
     */
    public static final class Builder<T> {

        private static final long INITIAL_RETRY_TIMEOUT_MS = 100L;
        private static final long MAXIMUM_RETRY_TIMEOUT_MS = 3_000L;
        private static final double EXPONENTIAL_BACKOFF_MULTIPLIER = 2.0;

        private static final RetryStrategy DEFAULT_RETRY_STRATEGY = RetryStrategies.custom()
                .intervalFunction(IntervalFunction.exponentialBackoffWithCap(
                        INITIAL_RETRY_TIMEOUT_MS, EXPONENTIAL_BACKOFF_MULTIPLIER, MAXIMUM_RETRY_TIMEOUT_MS))
                .build();

        @Nonnull
        private final String stream;
        @Nonnull
        private final FunctionEx<T, String> keyFn;
        @Nonnull
        private final FunctionEx<T, byte[]> valueFn;
        @Nonnull
        private final AwsConfig awsConfig = new AwsConfig();
        @Nonnull
        private RetryStrategy retryStrategy = DEFAULT_RETRY_STRATEGY;

        /**
         * TODO: javadoc
         */
        private Builder(
                @Nonnull String stream,
                @Nonnull FunctionEx<T, String> keyFn,
                @Nonnull FunctionEx<T, byte[]> valueFn
        ) {
            this.stream = stream;
            this.keyFn = keyFn;
            this.valueFn = valueFn;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withEndpoint(@Nullable String endpoint) {
            awsConfig.withEndpoint(endpoint);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withRegion(@Nullable String region) {
            awsConfig.withRegion(region);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            awsConfig.withCredentials(accessKey, secretKey);
            return this;
        }

        /**
         * TODO: javadoc
         *
         * TODO: document how this differs from Amazon SDK ClientConfiguration and the retry policies within
         */
        @Nonnull
        public Builder<T> withRetryStrategy(@Nonnull RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Sink<T> build() {
            String name = "Kinesis Sink (" + stream + ")";
            KinesisSinkPSupplier<T> supplier =
                    new KinesisSinkPSupplier<>(awsConfig, stream, keyFn, valueFn, retryStrategy);
            return new SinkImpl<>(name, ProcessorMetaSupplier.of(supplier), DISTRIBUTED_PARTITIONED, keyFn);
        }
    }

}
