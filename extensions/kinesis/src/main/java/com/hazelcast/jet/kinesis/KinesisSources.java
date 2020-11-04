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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Contains factory methods for creating steaming sources based on
 * Amazon Kinesis Data Streams.
 *
 * @since 4.4
 */
public final class KinesisSources {

    private KinesisSources() {
    }

    /**
     * TODO: javadoc
     */
    @Nonnull
    public static Builder kinesis(@Nonnull String stream) {
        return new Builder(stream);
    }

    /**
     * TODO: javadoc
     */
    public static final class Builder {

        @Nonnull
        private final String stream;

        @Nullable
        private String endpoint;
        @Nullable
        private String region;
        @Nullable
        private String accessKey;
        @Nullable
        private String secretKey;

        /**
         * TODO: javadoc
         */
        private Builder(@Nonnull String stream) {
            this.stream = stream;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withEndpoint(@Nullable String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withRegion(@Nullable String region) {
            this.region = region;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public StreamSource<Map.Entry<String, byte[]>> build() {
            AwsConfig awsConfig = new AwsConfig(endpoint, region, accessKey, secretKey);
            return Sources.streamFromProcessor("Kinesis Source (" + stream + ")",
                    new KinesisSourcePMetaSupplier(awsConfig, stream));
        }
    }

}
