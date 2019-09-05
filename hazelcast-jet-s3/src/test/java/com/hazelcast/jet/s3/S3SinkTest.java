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
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.Predicates.alwaysTrue;
import static org.junit.Assert.assertEquals;

@Category(NightlyTest.class)
public class S3SinkTest extends JetTestSupport {

    private static String bucketName = "jet-s3-connector-test-bucket-sink";
    private static AmazonS3 client;

    @BeforeClass
    public static void setup() {
        client = client();
    }

    @Before
    @After
    public void deleteObjects() {
        String[] keys = client
                .listObjects(bucketName)
                .getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .toArray(String[]::new);
        if (keys.length > 0) {
            client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
        }
    }

    @Test
    public void test() {
        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();
        IMapJet<Integer, String> map = instance1.getMap("map");

        int itemCount = 1000;

        for (int i = 0; i < itemCount; i++) {
            map.put(i, "foo-" + i);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map, alwaysTrue(), Map.Entry::getValue))
         .drainTo(S3Sinks.s3(bucketName, S3SinkTest::client));

        instance1.newJob(p).join();

        ObjectListing listing = client.listObjects(bucketName);
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        assertEquals(2, objectSummaries.size());

        long totalLineCount = objectSummaries
                .stream()
                .map(summary -> client.getObject(bucketName, summary.getKey()))
                .mapToLong(S3SinkTest::lineCount)
                .sum();

        assertEquals(itemCount, totalLineCount);
    }

    static AmazonS3 client() {
        return AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    static long lineCount(S3Object s3Object) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            return reader.lines().count();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
