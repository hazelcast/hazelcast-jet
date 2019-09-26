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
import com.hazelcast.test.annotation.NightlyTest;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Category(NightlyTest.class)
public class S3SourceTest extends S3TestBase {

    private static final String BUCKET_NAME = "jet-s3-connector-test-bucket-source";
    private static final String BUCKET_NAME_2 = "jet-s3-connector-test-bucket-source-2";

    private static final int LINE_COUNT = 1000;

    @Test
    public void when_readWholeBucket() {
        testSource(BUCKET_NAME, null, 1100, LINE_COUNT);
    }

    @Test
    public void when_readWithPrefix() {
        testSource(BUCKET_NAME, "file-9", 111, LINE_COUNT);
    }

    @Test
    public void when_readFromFolder() {
        testSource(BUCKET_NAME_2, "testFolder", 3, LINE_COUNT);
    }

    @Test
    public void when_readEmptyFolder() {
        testSourceWithEmptyResults(BUCKET_NAME_2, "emptyFolder");
    }

    @Test
    public void when_withNotExistingBucket() {
        testSourceWithNotExistingBucket("jet-s3-connector-test-bucket-source-THIS-BUCKET-DOES-NOT-EXIST");
    }

    @Test
    public void when_withNotExistingPrefix() {
        testSourceWithEmptyResults(BUCKET_NAME_2, "THIS-PREFIX-DOES-NOT-EXIST");
    }

    @Test
    public void when_withSpaceInName() {
        testSource(BUCKET_NAME_2, "file with space", 1, LINE_COUNT);
    }

    @Test
    public void when_withNonAsciiSymbolInName() {
        testSource(BUCKET_NAME_2, "测试", 1, LINE_COUNT);
    }

    @Test
    public void when_withNonAsciiSymbolInFile() {
        testSource(BUCKET_NAME_2, "fileWithNonASCIISymbol", 1, 1, "测试");
    }

    @Test
    public void when_withTwoBuckets() {
        List<String> buckets = new ArrayList<>();
        buckets.add(BUCKET_NAME);
        buckets.add(BUCKET_NAME_2);
        testSource(buckets, "file-999", 2, LINE_COUNT);
    }

    SupplierEx<S3Client> clientSupplier() {
        return () -> S3Client.builder()
                             .region(Region.US_EAST_1)
                             .build();
    }

}
