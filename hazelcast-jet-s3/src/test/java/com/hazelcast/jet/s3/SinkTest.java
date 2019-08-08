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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static com.amazonaws.regions.Regions.US_EAST_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SinkTest extends JetTestSupport {

    private static String bucketName = "jet-s3-connector-test-bucket-sink";
    private static String accessKeyId;
    private static String accessKeySecret;
    private static AmazonS3 client;

    @BeforeClass
    public static void setup() {
        accessKeyId = getSystemPropertyOrEnv("S3_ACCESS_KEY_ID");
        accessKeySecret = getSystemPropertyOrEnv("S3_ACCESS_KEY_SECRET");
        client = S3Utils.client(accessKeyId, accessKeySecret, US_EAST_1);
    }

    @Before
    public void deleteObjects() {
        client.createBucket(bucketName);
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
        int batchSize = 100;

        for (int i = 0; i < itemCount; i++) {
            map.put(i, "foo-" + i);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(map))
         .drainTo(S3Sinks.s3(bucketName, batchSize, accessKeyId, accessKeySecret, US_EAST_1, Map.Entry::getValue));

        instance1.newJob(p).join();

        ObjectListing listing = client.listObjects(bucketName);
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        assertTrue(objectSummaries.size() >= itemCount / batchSize);

        long totalLineCount = objectSummaries
                .stream()
                .map(summary -> client.getObject(bucketName, summary.getKey()))
                .mapToLong(SinkTest::lineCount)
                .sum();

        assertEquals(itemCount, totalLineCount);
    }

    private static long lineCount(S3Object s3Object) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            return reader.lines().count();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    static String getSystemPropertyOrEnv(String key) {
        String property = System.getProperty(key);
        return property != null ? property : System.getenv(key);
    }
}
