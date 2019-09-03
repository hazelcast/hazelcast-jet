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
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

/**
 * Contains factory methods for creating AWS S3 sources.
 */
public final class S3Sources {

    private static final int LOCAL_PARALLELISM = 2;

    private S3Sources() {
    }

    /**
     * Convenience for {@link #s3(List, String, Charset, SupplierEx, BiFunctionEx)}.
     * Emits lines to downstream without any transformation and uses {@link
     * StandardCharsets#UTF_8}.
     */
    @Nonnull
    public static BatchSource<String> s3(
            @Nonnull List<String> bucketNames,
            @Nullable String prefix,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier
    ) {
        return s3(bucketNames, prefix, UTF_8, clientSupplier, (name, line) -> line);
    }

    /**
     * Creates an AWS S3 {@link BatchSource} which lists all the objects in the
     * bucket-list using given {@code prefix}, reads them line by line,
     * transforms each line to the desired output object using given {@code
     * mapFn} and emits them to downstream.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * The default local parallelism for this processor is 2.
     * <p>
     * Here is an example which reads the objects from a single bucket with
     * applying the given prefix.
     *
     * <pre>{@code
     * BatchSource<String> batchSource =
     *      S3Sources.s3(
     *          Collections.singletonList("input-bucket"),
     *          "prefix",
     *          StandardCharsets.UTF_8,
     *          () -> {
     *              BasicAWSCredentials credentials =
     *                  new BasicAWSCredentials("accessKey", "accessKeySecret");
     *              return AmazonS3ClientBuilder
     *                     .standard()
     *                     .withCredentials(new AWSStaticCredentialsProvider(credentials))
     *                     .withRegion(Regions.US_EAST_1)
     *                     .build();
     *          },
     *          (name, line) -> line
     *      );
     * Pipeline p = Pipeline.create();
     * BatchStage<Document> srcStage = p.drawFrom(batchSource);
     * }</pre>
     *
     * @param bucketNames    list of bucket-names
     * @param prefix         the prefix to filter the objects
     * @param clientSupplier S3 client supplier
     * @param mapFn          the function which creates output object from each
     *                       line. Gets the object name and line as parameters
     * @param <T>            the type of the items the source emits
     */
    @Nonnull
    public static <T> BatchSource<T> s3(
            @Nonnull List<String> bucketNames,
            @Nullable String prefix,
            @Nonnull Charset charset,
            @Nonnull SupplierEx<? extends AmazonS3> clientSupplier,
            @Nonnull BiFunctionEx<String, String, ? extends T> mapFn
    ) {
        String charsetName = charset.name();
        return SourceBuilder
                .batch("s3-source", context ->
                        new S3Context<>(bucketNames, prefix, charsetName, context, clientSupplier, mapFn))
                .<T>fillBufferFn(S3Context::fillBuffer)
                .distributed(LOCAL_PARALLELISM)
                .destroyFn(S3Context::close)
                .build();
    }

    private static final class S3Context<T> {

        private static final ObjectListing EMPTY_LISTING = new ObjectListing();
        private static final int BATCH_COUNT = 1024;

        private final AmazonS3 amazonS3;
        private final Map<String, ObjectListing> listingMap;
        private final BiFunctionEx<String, String, ? extends T> mapFn;
        private final Charset charset;
        private final int processorIndex;
        private final int totalParallelism;

        private Iterator<S3ObjectSummary> iterator;
        private BufferedReader reader;
        private String objectName;

        private S3Context(
                List<String> bucketNames,
                String prefix,
                String charsetName,
                Context context,
                SupplierEx<? extends AmazonS3> clientSupplier,
                BiFunctionEx<String, String, ? extends T> mapFn
        ) {
            this.amazonS3 = clientSupplier.get();
            this.mapFn = mapFn;
            this.charset = Charset.forName(charsetName);
            this.processorIndex = context.globalProcessorIndex();
            this.totalParallelism = context.totalParallelism();
            this.listingMap = bucketNames
                    .stream()
                    .collect(toMap(key -> key, key -> amazonS3.listObjects(key, prefix)));
        }

        private void fillBuffer(SourceBuffer<? super T> buffer) throws IOException {
            if (reader != null) {
                addBatchToBuffer(buffer);
                return;
            }

            if (iterator == null) {
                // create an iterator for the object summaries which belongs to
                // this processor.
                iterator = createIterator();
                // iterator is empty, we've exhausted all the objects
                if (!iterator.hasNext()) {
                    buffer.close();
                    return;
                }
            }

            if (iterator.hasNext()) {
                S3ObjectSummary summary = iterator.next();
                S3Object s3Object = amazonS3.getObject(summary.getBucketName(), summary.getKey());
                objectName = s3Object.getKey();
                reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), charset));
                addBatchToBuffer(buffer);
            } else {
                iterator = null;
                listingMap.replaceAll((k, v) -> v.isTruncated() ? amazonS3.listNextBatchOfObjects(v) : EMPTY_LISTING);
            }
        }

        private void addBatchToBuffer(SourceBuffer<? super T> buffer) throws IOException {
            for (int i = 0; i < BATCH_COUNT; i++) {
                String line = reader.readLine();
                if (line == null) {
                    reader.close();
                    reader = null;
                    return;
                }
                buffer.add(mapFn.apply(objectName, line));
            }
        }

        private Iterator<S3ObjectSummary> createIterator() {
            return listingMap
                    .values()
                    .stream()
                    .flatMap(listing -> listing.getObjectSummaries().stream())
                    .filter(summary -> belongsToThisProcessor(summary.getKey()))
                    .iterator();
        }


        private boolean belongsToThisProcessor(String key) {
            return Math.floorMod(key.hashCode(), totalParallelism) == processorIndex;
        }

        private void close() {
            amazonS3.shutdown();
        }
    }
}
