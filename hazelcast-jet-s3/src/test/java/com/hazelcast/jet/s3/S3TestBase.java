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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.junit.Before;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.GenericPredicates.alwaysTrue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toInputStream;

abstract class S3TestBase extends JetTestSupport {

    JetInstance jet;

    @Before
    public void setupCluster() {
        jet = createJetMember();
        createJetMember();
    }

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
         .drainTo(S3Sinks.s3(bucketName, prefix, UTF_8, clientSupplier(), Object::toString));

        jet.newJob(p).join();

        try (S3Client client = clientSupplier().get()) {
            long lineCount = client
                    .listObjects(req -> req.bucket(bucketName).prefix(prefix))
                    .contents()
                    .stream()
                    .map(o -> client.getObject(req -> req.bucket(bucketName).key(o.key()), toInputStream()))
                    .flatMap(this::inputStreamToLines)
                    .peek(line -> assertEquals(payload, line))
                    .count();
            assertEquals(itemCount, lineCount);
        }
    }

    void testSource(JetInstance jet, String bucketName, String prefix, int objectCount, int lineCount) {
        Pipeline p = Pipeline.create();
        p.drawFrom(S3Sources.s3(singletonList(bucketName), prefix, clientSupplier()))
         .groupingKey(s -> s)
         .aggregate(AggregateOperations.counting())
         .apply(Assertions.assertCollected(entries -> {
             assertTrue(entries.stream().allMatch(
                     e -> e.getValue() == objectCount && e.getKey().matches("^line\\-\\d+$")
             ));
             assertEquals(lineCount, entries.size());
         }));

        jet.newJob(p).join();
    }

    abstract SupplierEx<S3Client> clientSupplier();

    void deleteBucket(S3Client client, String bucket) {
        try {
            client.deleteBucket(b -> b.bucket(bucket));
        } catch (NoSuchBucketException ignored) {
        }
    }

    Stream<String> inputStreamToLines(InputStream is) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            // materialize the stream, since we can't read it afterwards
            return reader.lines().collect(Collectors.toList()).stream();
        } catch (IOException e) {
            throw new AssertionError("Error reading file ", e);
        }
    }

}
