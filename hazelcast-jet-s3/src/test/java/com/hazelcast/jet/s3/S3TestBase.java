package com.hazelcast.jet.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.Assertions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.pipeline.GenericPredicates.alwaysTrue;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

abstract class S3TestBase extends JetTestSupport {

    void testSink(JetInstance jet, String bucketName) {
        IMapJet<Integer, String> map = jet.getMap("map");

        int itemCount = 20000;
        String prefix = "my-objects-";
        String payload = generateRandomString(1_000);

        for (int i = 0; i < itemCount; i++) {
            map.put(i, payload);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map, alwaysTrue(), Map.Entry::getValue))
         .drainTo(S3Sinks.s3(bucketName, prefix, StandardCharsets.UTF_8, client(), Object::toString));

        jet.newJob(p).join();

        AmazonS3 client = client().get();
        ObjectListing listing = client.listObjects(bucketName);
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        assertEquals(2, objectSummaries.size());

        long totalLineCount = objectSummaries
                .stream()
                .filter(summary -> summary.getKey().startsWith(prefix))
                .map(summary -> client.getObject(bucketName, summary.getKey()))
                .mapToLong(value -> assertPayloadAndCount(value, payload))
                .sum();

        assertEquals(itemCount, totalLineCount);
    }

    void testSource(JetInstance jet, String bucketName, String prefix, int objectCount, int lineCount) {
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(bucketName), prefix, client()))
         .groupingKey(s -> s)
         .aggregate(AggregateOperations.counting())
         .apply(Assertions.assertCollected(entries -> {
             assertTrue(entries.stream().allMatch(e -> e.getValue() == objectCount && e.getKey().matches("^line\\-\\d+$")));
             assertEquals(lineCount, entries.size());
         }));

        jet.newJob(p).join();
    }

    abstract SupplierEx<AmazonS3> client();

    static long assertPayloadAndCount(S3Object s3Object, String expectedPayload) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            return reader.lines().peek(s -> assertEquals(expectedPayload, s)).count();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }


}
