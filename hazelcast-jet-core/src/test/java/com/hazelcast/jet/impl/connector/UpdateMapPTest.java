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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;

@RunWith(HazelcastParallelClassRunner.class)
public class UpdateMapPTest extends JetTestSupport {

    private JetInstance jet;
    private IMapJet<String, Integer> sinkMap;

    @Before
    public void setup() {
        jet = createJetMember();
        sinkMap = jet.getMap("results");
    }

    @Test
    public void test() {
        SupplierEx<Processor> sup = () -> new UpdateMapP<Integer, String, Integer>(
            jet.getHazelcastInstance(),
            1,
            true,
            sinkMap.getName(),
            Object::toString,
            (prev, next) -> {
                if (prev == null) {
                    return 1;
                }
                return prev + 1;
            });

        TestSupport
            .verifyProcessor(sup)
            .jetInstance(jet)
            .input(Arrays.asList(1, 2, 3, 4, 1, 2, 3, 4))
            .disableSnapshots()
            .disableLogging()
            .disableProgressAssertion()
            .expectOutput(Collections.emptyList());

        System.out.println(sinkMap.entrySet());
    }
}
