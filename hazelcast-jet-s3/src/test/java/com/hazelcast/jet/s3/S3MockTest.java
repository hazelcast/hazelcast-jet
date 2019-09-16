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

import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.s3.S3Sinks.S3SinkContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static java.lang.System.lineSeparator;
import static java.util.stream.IntStream.range;

public class S3MockTest extends S3TestBase {

    @ClassRule
    public static S3MockContainer s3MockContainer = new S3MockContainer();

    private static final ILogger logger = Logger.getLogger(S3MockTest.class);
    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SOURCE_BUCKET_2 = "source-bucket-2";
    private static final String SOURCE_BUCKET_EMPTY = "source-bucket-empty";
    private static final String SINK_BUCKET = "sink-bucket";
    private static final String SINK_BUCKET_OVERWRITE = "sink-bucket-overwrite";
    private static final String SINK_BUCKET_NONASCII = "sink-bucket-nonascii";

    private static final int LINE_COUNT = 100;

    private static S3Client s3Client;

    @BeforeClass
    public static void setupS3() {
        S3SinkContext.maximumPartNumber = 1;
        s3MockContainer.followOutput(outputFrame -> logger.info(outputFrame.getUtf8String().trim()));
        s3Client = s3MockContainer.client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(SOURCE_BUCKET).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(SOURCE_BUCKET_2).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(SOURCE_BUCKET_EMPTY).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(SINK_BUCKET).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(SINK_BUCKET_OVERWRITE).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(SINK_BUCKET_NONASCII).build());
    }

    @Before
    public void generateData() {
        generateAndUploadObjects(SOURCE_BUCKET, "object-", 20, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "object-", 4, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "testFolder/object-", 5, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "测试", 1, LINE_COUNT);
        generateAndUploadObjects(SOURCE_BUCKET_2, "fileWithNonASCIISymbol", 1, LINE_COUNT, "测试-");
    }

    @AfterClass
    public static void teardown() {
        s3Client.close();
        S3SinkContext.maximumPartNumber = S3SinkContext.DEFAULT_MAXIMUM_PART_NUMBER;
    }

    @Test
    public void testMockSink() {
        testSink(SINK_BUCKET);
    }

    @Test
    public void testMockSource() {
        testSource(SOURCE_BUCKET, null, 20, LINE_COUNT);
    }

    @Test
    public void testSourceWithPrefix() {
        testSource(SOURCE_BUCKET, "object-1", 11, LINE_COUNT);
    }

    @Test
    public void testSourceReadFromFolder() {
        testSource(SOURCE_BUCKET_2, "testFolder", 5, LINE_COUNT);
    }

    @Test
    public void testSourceWithNotExistingBucket() {
        testSourceWithNotExistingBucket("jet-s3-connector-test-bucket-source-THIS-BUCKET-DOES-NOT-EXIST");
    }

    @Test
    public void testSourceWithNotExistingPrefix() {
        testSourceWithEmptyResults(SOURCE_BUCKET_2, "THIS-PREFIX-DOES-NOT-EXIST");
    }

    @Test
    public void testSourceWithEmptyBucket() {
        testSourceWithEmptyResults(SOURCE_BUCKET_EMPTY, null);
    }

    @Test
    public void testSourceWithTwoBuckets() {
        List<String> buckets = new ArrayList<>();
        buckets.add(SOURCE_BUCKET);
        buckets.add(SOURCE_BUCKET_2);
        testSource(buckets, "object-3", 2, LINE_COUNT);
    }

    @Test
    public void testSinkOverwritesFile() {
        testSink(SINK_BUCKET_OVERWRITE, "my-objects-", 100);
        testSink(SINK_BUCKET_OVERWRITE, "my-objects-", 200);
    }

    @Test
    public void testDrainToNotExistingBucket() {
        testSinkWithNotExistingBucket("jet-s3-connector-test-bucket-sink-THIS-BUCKET-DOES-NOT-EXIST");
    }

    @Test
    public void testSourceWithNonAsciiSymbolInName() {
        testSource(SOURCE_BUCKET_2, "测试", 1, LINE_COUNT);
    }

    @Test
    public void testSourceWithNonAsciiSymbolInFile() {
        testSource(SOURCE_BUCKET_2, "fileWithNonASCIISymbol", 1, LINE_COUNT, "^测试\\-\\d+$");
    }

    @Test
    public void testSinkWithNonAsciiSymbolInName() {
        testSink(SINK_BUCKET_NONASCII, "测试", 10);
    }

    @Test
    public void testSinkWithNonAsciiSymbolInFile() {
        testSink(SINK_BUCKET_NONASCII, "fileWithNonAsciiSymbol", 10, "测试");
    }

    SupplierEx<S3Client> clientSupplier() {
        return () -> S3MockContainer.client(s3MockContainer.endpointURL());
    }

    private void generateAndUploadObjects(String bucketName, String prefix, int objectCount, int lineCount) {
        generateAndUploadObjects(bucketName, prefix, objectCount, lineCount, "line-");
    }

    private void generateAndUploadObjects(String bucketName, String prefix, int objectCount, int lineCount,
            String textPrefix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < objectCount; i++) {
            range(0, lineCount).forEach(j -> builder.append(textPrefix).append(j).append(lineSeparator()));
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(prefix + i)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromString(builder.toString()));
            builder.setLength(0);
        }
    }
}
