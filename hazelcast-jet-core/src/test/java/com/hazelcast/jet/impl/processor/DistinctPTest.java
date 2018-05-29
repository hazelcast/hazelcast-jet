/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestSupport;
import org.junit.Test;

import static com.hazelcast.jet.function.DistributedFunction.identity;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class DistinctPTest {

    @Test
    public void smokeTest() {
        TestSupport.verifyProcessor(Processors.distinctP(identity()))
                   .disableSnapshots()
                   .input(asList(1, 2, 3, 2, 1, 4))
                   .expectOutput(asList(1, 2, 3, 4));
    }

    @Test
    public void testEmpty() {
        TestSupport.verifyProcessor(Processors.distinctP(identity()))
                   .disableSnapshots()
                   .input(emptyList())
                   .expectOutput(emptyList());
    }
}
