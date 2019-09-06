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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class S3SinkTest extends S3TestBase {

    private static String bucketName = "jet-s3-connector-test-bucket-sink";

    @Before
    @After
    public void deleteObjects() {
        AmazonS3 client = client().get();
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
        testSink(instance2, bucketName);
    }

    SupplierEx<AmazonS3> client() {
        return () -> AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

}
