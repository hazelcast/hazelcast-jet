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

    private KinesisSinks() {
    }

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

        @Nonnull
        private final String stream;
        @Nonnull
        private final FunctionEx<T, String> keyFn;
        @Nonnull
        private final FunctionEx<T, byte[]> valueFn;
        @Nonnull
        private final AwsConfig config = new AwsConfig();

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
            this.config.setEndpoint(endpoint);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withRegion(@Nullable String region) {
            this.config.setRegion(region);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            this.config.setCredentials(accessKey, secretKey);
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Sink<T> build() {
            String name = "Kinesis Sink (" + stream + ")";
            KinesisSinkPSupplier<T> supplier = new KinesisSinkPSupplier<>(config, stream, keyFn, valueFn);
            return new SinkImpl<>(name, ProcessorMetaSupplier.of(supplier), DISTRIBUTED_PARTITIONED, keyFn);
        }
    }

}
