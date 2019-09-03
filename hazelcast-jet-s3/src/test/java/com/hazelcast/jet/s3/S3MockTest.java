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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.s3.S3MockContainer.client;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3MockTest extends JetTestSupport {

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SINK_BUCKET = "sink-bucket";

    @ClassRule
    public static S3MockContainer s3MockContainer = new S3MockContainer();

    private static AmazonS3 s3Client;
    private static long totalLineCount;
    private JetInstance jet;

    @BeforeClass
    public static void setupS3() throws IOException {
        s3Client = s3MockContainer.client();
        s3Client.createBucket(SOURCE_BUCKET);
        s3Client.createBucket(SINK_BUCKET);
        uploadBooks();
    }

    @Before
    public void setup() {
        jet = createJetMembers(2)[0];
    }

    @Test
    public void testSink() {
        IMapJet<Integer, String> map = jet.getMap("map");

        int itemCount = 1000;
        int batchSize = 100;

        for (int i = 0; i < itemCount; i++) {
            map.put(i, "foo-" + i);
        }

        String endpointURL = s3MockContainer.endpointURL();
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map))
         .drainTo(S3Sinks.s3(SINK_BUCKET, batchSize, () -> client(endpointURL), Map.Entry::getValue));

        jet.newJob(p).join();

        ObjectListing listing = s3Client.listObjects(SINK_BUCKET);
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        assertTrue(objectSummaries.size() >= itemCount / batchSize);

        long totalLineCount = objectSummaries
                .stream()
                .map(summary -> s3Client.getObject(SINK_BUCKET, summary.getKey()))
                .mapToLong(S3SinkTest::lineCount)
                .sum();

        assertEquals(itemCount, totalLineCount);
    }

    @Test
    public void testSource() {
        long localTotalLineCount = totalLineCount;

        String endpointURL = s3MockContainer.endpointURL();
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(SOURCE_BUCKET), null, defaultCharset(),
                () -> client(endpointURL), (name, line) -> line))
         .aggregate(AggregateOperations.counting())
         .drainTo(AssertionSinks.assertCollectedEventually(10, list -> {
             long sum = list.stream().mapToLong(l -> l).sum();
             Assert.assertEquals(localTotalLineCount, sum);
         }));

        try {
            jet.newJob(p).join();
        } catch (CompletionException e) {
            assertTrue(e.getCause().getCause() instanceof AssertionCompletedException);
        }
    }

    private static void uploadBooks() throws IOException {
        Path path = Paths.get(S3MockTest.class.getResource("/books").getPath());
        Files.list(path)
             .filter(book -> book.getFileName().toString().startsWith("a"))
             .forEach(book -> {
                 s3Client.putObject(SOURCE_BUCKET, book.getFileName().toString(), book.toFile());
                 totalLineCount += uncheckCall(() -> Files.lines(book).count());
                 System.out.println("uploaded file " + book.getFileName().toString() + " path: " + book);
             });
    }

}
