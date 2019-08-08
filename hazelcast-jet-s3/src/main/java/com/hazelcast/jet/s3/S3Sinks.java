/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for creating AWS S3 sinks.
 */
public final class S3Sinks {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private S3Sinks() {
    }

    /**
     * Convenience for {@link #s3(String, int, SupplierEx, FunctionEx)}.
     * Creates an S3 client with given credentials and region. Uses {@link
     * Object#toString()} for {@code toStringFn}.
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            @Nonnull String accessKeyId,
            @Nonnull String accessKeySecret,
            @Nonnull Regions region
    ) {
        return s3(bucketName, DEFAULT_BATCH_SIZE, accessKeyId, accessKeySecret, region, Object::toString);
    }

    /**
     * Convenience for {@link #s3(String, int, SupplierEx, FunctionEx)}.
     * Creates an S3 client with given credentials and region.
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            int batchSize,
            @Nonnull String accessKeyId,
            @Nonnull String accessKeySecret,
            @Nonnull Regions region,
            @Nonnull FunctionEx<? super T, String> toStringFn
    ) {
        return s3(bucketName, batchSize, () -> S3Utils.client(accessKeyId, accessKeySecret, region), toStringFn);
    }

    /**
     * Creates an AWS S3 {@link Sink} which writes items to files into the
     * given bucket. Sink converts each item to string using given {@code
     * toStringFn} and writes it as a line. Each processor will create a file
     * for a batch of items. Name of the file will be processor's global index
     * followed by an incremented counter, for example the processor having the
     * index 2 will create these consecutive files {@code 2-0, 2-1, 2-2 ...}.
     * <p>
     * No state is saved to snapshot for this sink. If the job is restarted
     * previously written files will be overwritten.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param bucketName     the name of the bucket
     * @param batchSize      the size of the batch
     * @param clientSupplier S3 client supplier
     * @param toStringFn     the function which converts each item to its
     *                       string representation
     * @param <T>            type of the items the sink accepts
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            int batchSize,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier,
            @Nonnull FunctionEx<? super T, String> toStringFn
    ) {
        return SinkBuilder
                .sinkBuilder("s3-sink", context ->
                        new S3Context<>(bucketName, context.globalProcessorIndex(), batchSize, clientSupplier, toStringFn))
                .<T>receiveFn(S3Context::receive)
                .destroyFn(S3Context::close)
                .build();
    }

    private static class S3Context<T> {

        final String bucketName;
        final int processorIndex;
        final int batchSize;
        final AmazonS3 amazonS3;
        final FunctionEx<? super T, String> toStringFn;


        int itemCounter;
        long objectCounter;
        StringBuilder buffer = new StringBuilder();

        S3Context(
                String bucketName,
                int processorIndex,
                int batchSize,
                SupplierEx<? extends AmazonS3> clientSupplier,
                FunctionEx<T, String> toStringFn
        ) {
            this.bucketName = bucketName;
            this.processorIndex = processorIndex;
            this.batchSize = batchSize;
            this.amazonS3 = clientSupplier.get();
            this.toStringFn = toStringFn;
        }

        void receive(T item) {
            buffer.append(toStringFn.apply(item));
            buffer.append(System.lineSeparator());
            if (++itemCounter == batchSize) {
                amazonS3.putObject(bucketName, nextKey(), buffer.toString());
                buffer = new StringBuilder();
                itemCounter = 0;
            }
        }

        void close() {
            if (buffer.length() > 0) {
                amazonS3.putObject(bucketName, nextKey(), buffer.toString());
            }
            amazonS3.shutdown();
        }


        private String nextKey() {
            return processorIndex + "-" + (++objectCounter);
        }
    }
}
