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

public class KinesisSinks {

    //todo, limitation on batch put records: Each PutRecords request can support
    // up to 500 records. Each record in the request can be as large as 1 MiB,
    // up to a limit of 5 MiB for the entire request, including partition keys.
    // Each shard can support writes up to 1,000 records per second, up to a
    // maximum data write total of 1 MiB per second.

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

    public static final class Builder<T> {

        @Nonnull
        private final String stream;
        @Nonnull
        private final FunctionEx<T, String> keyFn;
        @Nonnull
        private final FunctionEx<T, byte[]> valueFn;

        @Nullable
        String endpoint;
        @Nullable
        String region;
        @Nullable
        String accessKey;
        @Nullable
        String secretKey;

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
            this.endpoint = endpoint;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withRegion(@Nullable String region) {
            this.region = region;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Builder<T> withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            return this;
        }

        /**
         * TODO: javadoc
         */
        @Nonnull
        public Sink<T> build() {
            AwsConfig awsConfig = new AwsConfig(endpoint, region, accessKey, secretKey);
            String name = "Kinesis Sink (" + stream + ")";
            KinesisSinkPSupplier<T> supplier = new KinesisSinkPSupplier<>(awsConfig, stream, keyFn, valueFn);
            return new SinkImpl<>(name, ProcessorMetaSupplier.of(supplier), DISTRIBUTED_PARTITIONED, keyFn);
        }
    }

}
