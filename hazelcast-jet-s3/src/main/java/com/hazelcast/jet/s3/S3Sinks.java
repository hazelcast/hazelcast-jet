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

import com.amazonaws.services.s3.AmazonS3;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.util.StringUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Contains factory methods for creating AWS S3 sinks.
 */
public final class S3Sinks {

    private S3Sinks() {
    }

    /**
     * Convenience for {@link #s3(String, int, String, SupplierEx, FunctionEx)}
     * Uses {@link Object#toString()} to convert the items to lines.
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            int linesPerFile,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier
    ) {
        return s3(bucketName, linesPerFile, null, clientSupplier, Object::toString);
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
     * <p>
     * Here is an example which reads from a map and writes the values of the
     * entries to given bucket using {@link Object#toString()} to convert the
     * values to a line.
     *
     * <pre>{@code
     * Sink<? super Object> sink = S3Sinks.s3(
     *         "bucket",
     *         1024,
     *         () -> {
     *             BasicAWSCredentials credentials =
     *                     new BasicAWSCredentials("accessKey", "accessKeySecret");
     *             return AmazonS3ClientBuilder
     *                     .standard()
     *                     .withCredentials(new AWSStaticCredentialsProvider(credentials))
     *                     .withRegion(Regions.US_EAST_1)
     *                     .build();
     *         },
     *         Object::toString
     * );
     * Pipeline p = Pipeline.create();
     * p.drawFrom(Sources.map("map", alwaysTrue(), Map.Entry::getValue))
     *  .drainTo(sink);
     * }</pre>
     *
     * @param bucketName     the name of the bucket
     * @param linesPerFile   the number of lines per file
     * @param clientSupplier S3 client supplier
     * @param toStringFn     the function which converts each item to its
     *                       string representation
     * @param <T>            type of the items the sink accepts
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            int linesPerFile,
            @Nullable String prefix,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier,
            @Nonnull FunctionEx<? super T, String> toStringFn
    ) {
        return SinkBuilder
                .sinkBuilder("s3-sink", context ->
                        new S3Context<>(bucketName, prefix, context.globalProcessorIndex(),
                                linesPerFile, clientSupplier, toStringFn))
                .<T>receiveFn(S3Context::receive)
                .destroyFn(S3Context::close)
                .build();
    }

    private static final class S3Context<T> {

        private final String bucketName;
        private final String prefix;
        private final int processorIndex;
        private final int linesPerFile;
        private final AmazonS3 amazonS3;
        private final FunctionEx<? super T, String> toStringFn;

        private int itemCounter;
        private long objectCounter;
        private StringBuilder buffer = new StringBuilder();

        private S3Context(
                String bucketName,
                String prefix,
                int processorIndex,
                int linesPerFile,
                SupplierEx<? extends AmazonS3> clientSupplier,
                FunctionEx<? super T, String> toStringFn
        ) {
            this.bucketName = bucketName;
            this.prefix = StringUtil.isNullOrEmptyAfterTrim(prefix) ? "" : prefix;
            this.processorIndex = processorIndex;
            this.linesPerFile = linesPerFile;
            this.amazonS3 = clientSupplier.get();
            this.toStringFn = toStringFn;

            checkIfBucketExists();
        }

        private void checkIfBucketExists() {
            if (!amazonS3.doesBucketExistV2(bucketName)) {
                throw new IllegalArgumentException("Bucket [" + bucketName + "] does not exist");
            }
        }

        private void receive(T item) {
            buffer.append(toStringFn.apply(item))
                  .append(System.lineSeparator());
            if (++itemCounter == linesPerFile) {
                amazonS3.putObject(bucketName, nextKey(), buffer.toString());
                buffer.setLength(0);
                itemCounter = 0;
            }
        }

        private void close() {
            try {
                if (buffer.length() > 0) {
                    amazonS3.putObject(bucketName, nextKey(), buffer.toString());
                }
            } finally {
                amazonS3.shutdown();
            }
        }


        private String nextKey() {
            return prefix + processorIndex + "-" + (++objectCounter);
        }
    }
}
