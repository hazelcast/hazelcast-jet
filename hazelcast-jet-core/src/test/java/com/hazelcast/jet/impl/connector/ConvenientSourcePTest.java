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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.util.MutableInteger;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.fail;

public class ConvenientSourcePTest {

    @Test
    public void smokeTest() {
        long totalCount = 5L;
        BatchSource<Integer> source = SourceBuilder
                .batch("src", ctx -> new MutableInteger())
                .<Integer>fillBufferFn((src, buffer) -> {
                    for (;;) {
                        if (src.value == totalCount) {
                            buffer.close();
                            break;
                        }
                        buffer.add(src.getAndInc());
                    }
                })
                .createSnapshotFn(src -> src.value)
                .restoreSnapshotFn((src, states) -> {
                    assert states.size() == 1;
                    src.value = states.get(0);
                })
                .distributed(1) // we use this to avoid forceTotalParallelismOne
                .build();

        TestSupport
                .verifyProcessor(((BatchSourceTransform<Integer>) source).metaSupplier)
                .expectOutput(Arrays.asList(0, 1, 2, 3, 4));
    }

    @Test
    public void test_nullState() {
        long totalCount = 5L;
        int[] curvalue = {0};
        BatchSource<Integer> source = SourceBuilder
                .batch("src", ctx -> "foo")
                .<Integer>fillBufferFn((src, buffer) -> {
                    for (;;) {
                        if (curvalue[0] == totalCount) {
                            buffer.close();
                            curvalue[0] = 0; // reset value for next test
                            break;
                        }
                        buffer.add(curvalue[0]++);
                    }
                })
                .createSnapshotFn(src -> null)
                .restoreSnapshotFn((src, states) -> fail("nothing should have been restored"))
                .distributed(1) // we use this to avoid forceTotalParallelismOne
                .build();

        TestSupport
                .verifyProcessor(((BatchSourceTransform<Integer>) source).metaSupplier)
                .expectOutput(Arrays.asList(0, 1, 2, 3, 4));
    }
}
