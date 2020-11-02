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

        @Nullable String endpoint;
        @Nullable String region;
        @Nullable String accessKey;
        @Nullable String secretKey;

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
            return Sources.streamFromProcessor("Kinesis(" + stream + ")",
                    new KinesisSourcePMetaSupplier(awsConfig, stream));
        }
    }

}
