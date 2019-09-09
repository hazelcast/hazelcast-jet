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
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.StringUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.services.s3.internal.Constants.MAXIMUM_UPLOAD_PARTS;
import static com.amazonaws.services.s3.internal.Constants.MB;

/**
 * Contains factory methods for creating AWS S3 sinks.
 */
public final class S3Sinks {

    private S3Sinks() {
    }

    /**
     * Convenience for {@link #s3(String, String, Charset, SupplierEx, FunctionEx)}
     * Uses {@link Object#toString()} to convert the items to lines.
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier
    ) {
        return s3(bucketName, null, StandardCharsets.UTF_8, clientSupplier, Object::toString);
    }

    /**
     * Creates an AWS S3 {@link Sink} which writes items to files into the
     * given bucket. Sink converts each item to string using given {@code
     * toStringFn} and writes it as a line. The sink creates a file
     * in the bucket for each processor instance. Name of the file will include
     * an user provided prefix (if defined) and processor's global index,
     * for example the processor having the
     * index 2 with prefix {@code my-object-} will create the object
     * {@code my-object-2}.
     * <p>
     * No state is saved to snapshot for this sink. If the job is restarted
     * previously written files will be overwritten.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * Here is an example which reads from a map and writes the entries
     * to given bucket using {@link Object#toString()} to convert the
     * values to a line.
     *
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * p.drawFrom(Sources.map("map"))
     *  .drainTo(S3Sinks.s3("bucket", "my-map-", StandardCharsets.UTF_8,
     *      () -> AmazonS3ClientBuilder.standard().build(),
     *      Object::toString
     * ));
     * }</pre>
     *
     * @param <T>            type of the items the sink accepts
     * @param bucketName     the name of the bucket
     * @param prefix         the prefix to be included in the file name
     * @param charset        the charset to be used when encoding the strings
     * @param clientSupplier S3 client supplier
     * @param toStringFn     the function which converts each item to its
     *                       string representation
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            @Nullable String prefix,
            @Nonnull Charset charset,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier,
            @Nonnull FunctionEx<? super T, String> toStringFn

    ) {
        String charsetName = charset.name();
        return SinkBuilder
                .sinkBuilder("s3-sink", context ->
                        new S3SinkContext<>(bucketName, prefix, context.globalProcessorIndex(),
                                clientSupplier, toStringFn, charsetName))
                .<T>receiveFn(S3SinkContext::receive)
                .flushFn(S3SinkContext::flush)
                .destroyFn(S3SinkContext::close)
                .build();
    }

    private static final class S3SinkContext<T> {

        private static final int DEFAULT_MINIMUM_UPLOAD_PART_SIZE = 5 * MB;
        private final String bucketName;
        private final String prefix;
        private final int processorIndex;
        private final AmazonS3 s3Client;
        private final FunctionEx<? super T, String> toStringFn;
        private final Charset charset;
        private final byte[] lineSeparator;

        private int partCounter;
        private int fileCounter;

        private ByteBuffer buffer = ByteBuffer.allocateDirect(2 * DEFAULT_MINIMUM_UPLOAD_PART_SIZE);
        private List<PartETag> partETags;
        private String uploadId;

        private S3SinkContext(
                String bucketName,
                String prefix,
                int processorIndex,
                SupplierEx<? extends AmazonS3> clientSupplier,
                FunctionEx<? super T, String> toStringFn,
                String charsetName) {
            this.bucketName = bucketName;
            this.prefix = StringUtil.isNullOrEmptyAfterTrim(prefix) ? "" : prefix;
            this.processorIndex = processorIndex;
            this.s3Client = clientSupplier.get();
            this.toStringFn = toStringFn;
            this.charset = Charset.forName(charsetName);
            this.lineSeparator = System.lineSeparator().getBytes(charset);

            checkIfBucketExists();
            initUploadRequest();
        }

        private void initUploadRequest() {
            partETags = new ArrayList<>();
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key());
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
            uploadId = initResponse.getUploadId();
        }

        private void checkIfBucketExists() {
            if (!s3Client.doesBucketExistV2(bucketName)) {
                throw new IllegalArgumentException("Bucket [" + bucketName + "] does not exist");
            }
        }

        private void receive(T item) {
            buffer.put(toStringFn.apply(item).getBytes(charset));
            buffer.put(lineSeparator);
        }

        private void flush() {
            if (partCounter > MAXIMUM_UPLOAD_PARTS) {
                completeActiveRequest();
                fileCounter++;
                partCounter = 0;
                initUploadRequest();
            }
            if (buffer.position() <= DEFAULT_MINIMUM_UPLOAD_PART_SIZE) {
                return;
            }
            UploadPartRequest uploadRequest = createUploadRequestFromBuffer();
            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
            partETags.add(uploadResult.getPartETag());
        }


        private void close() {
            try {
                completeActiveRequest();
            } finally {
                s3Client.shutdown();
            }
        }

        private void completeActiveRequest() {
            try {
                if (buffer.position() > 0) {
                    UploadPartRequest uploadRequest = createUploadRequestFromBuffer();
                    uploadRequest.withLastPart(true);
                    UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                    partETags.add(uploadResult.getPartETag());
                }
                if (partETags.isEmpty()) {
                    abortUpload();
                } else {
                    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, key(),
                            uploadId, partETags);
                    s3Client.completeMultipartUpload(compRequest);
                }
            } catch (Exception e) {
                abortUpload();
                ExceptionUtil.rethrow(e);
            }
        }

        private void abortUpload() {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key(), uploadId));
        }

        private UploadPartRequest createUploadRequestFromBuffer() {
            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            buffer.clear();
            int partSize = bytes.length;
            return new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key())
                    .withUploadId(uploadId)
                    .withPartNumber(++partCounter)
                    .withInputStream(new ByteArrayInputStream(bytes))
                    .withPartSize(partSize);
        }

        private String key() {
            return prefix + processorIndex + (fileCounter == 0 ? "" : "-" + fileCounter);
        }
    }
}
