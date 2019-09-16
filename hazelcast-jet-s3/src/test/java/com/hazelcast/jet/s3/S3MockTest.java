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
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.s3.S3Sinks.S3SinkContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toInputStream;

@RunWith(HazelcastSerialClassRunner.class)
public class S3MockTest extends S3TestBase {

    @ClassRule
    public static S3MockContainer s3MockContainer = new S3MockContainer();

    private static final ILogger logger = Logger.getLogger(S3MockTest.class);
    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SINK_BUCKET = "sink-bucket";

    private static S3Client s3Client;

    @BeforeClass
    public static void setupS3() {
        s3MockContainer.followOutput(outputFrame -> logger.info(outputFrame.getUtf8String().trim()));
        s3Client = s3MockContainer.client();
    }

    @AfterClass
    public static void teardown() {
        s3Client.close();
        S3SinkContext.maximumPartNumber = S3SinkContext.DEFAULT_MAXIMUM_PART_NUMBER;
    }

    @Before
    public void setup() {
        deleteBucket(s3Client, SOURCE_BUCKET);
        deleteBucket(s3Client, SINK_BUCKET);
    }

    @Test
    public void when_manySmallItems() {
        S3SinkContext.maximumPartNumber = 1;
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET));

        testSink(jet, SINK_BUCKET);
    }

    @Test
    public void when_itemsLargerThanBuffer() {
        s3Client.createBucket(b -> b.bucket(SINK_BUCKET));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < S3SinkContext.BUFFER_SIZE / 10; i++) {
            sb.append("012345678901234567890");
        }
        String expected = sb.toString();

        Pipeline p = Pipeline.create();
        p.drawFrom(TestSources.items(expected))
         .drainTo(S3Sinks.s3(SINK_BUCKET, null, UTF_8, clientSupplier(), Object::toString));

        jet.newJob(p).join();

        try (S3Client client = clientSupplier().get()) {
            List<String> lines = client
                    .listObjects(req -> req.bucket(SINK_BUCKET))
                    .contents()
                    .stream()
                    .peek(o -> System.out.println(o))
                    .map(object -> client.getObject(req -> req.bucket(SINK_BUCKET).key(object.key()), toInputStream()))
                    .flatMap(this::inputStreamToLines)
                    .collect(Collectors.toList());

            assertEquals(1, lines.size());
            String actual = lines.get(0);
            assertEquals(expected.length(), actual.length());
            assertEquals(expected, actual);
        }
    }


    @Test
    public void when_simpleSource() {
        s3Client.createBucket(b -> b.bucket(SOURCE_BUCKET));

        int objectCount = 20;
        int lineCount = 100;
        generateAndUploadObjects(objectCount, lineCount);

        testSource(jet, SOURCE_BUCKET, "object-", objectCount, lineCount);
    }

    SupplierEx<S3Client> clientSupplier() {
        return () -> S3MockContainer.client(s3MockContainer.endpointURL());
    }


    private void generateAndUploadObjects(int objectCount, int lineCount) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < objectCount; i++) {
            range(0, lineCount).forEach(j -> builder.append("line-").append(j).append(lineSeparator()));
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                                                                .bucket(SOURCE_BUCKET)
                                                                .key("object-" + i)
                                                                .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromString(builder.toString()));
            builder.setLength(0);
        }
    }
}
