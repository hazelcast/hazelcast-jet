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
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class UpdateMapWithEntryProcessorPTest extends JetTestSupport {

    private JetInstance jet;
    private IMapJet<String, Integer> sinkMap;
    private JetInstance client;

    @Before
    public void setup() {
        jet = createJetMember();
        client = createJetClient();
        sinkMap = jet.getMap("results");
    }

    @Test
    public void test_localMap() {
        runTest(jet,  1,true);
    }

    @Test
    public void test_localMap_highAsync() {
        runTest(jet,  1024,true);
    }

    @Test
    public void test_remoteMap() {
        runTest(client, 1, true);
    }

    @Test
    public void test_remoteMap_highAsync() {
        runTest(client, 1024, true);
    }

    private void runTest(JetInstance instance, int asyncLimit, boolean isLocal) {
        SupplierEx<Processor> sup = () -> new UpdateMapWithEntryProcessorP<Integer, String, Integer>(
            instance.getHazelcastInstance(),
            asyncLimit,
            isLocal,
            sinkMap.getName(),
            Object::toString,
            i -> new IncrementEntryProcessor());

        int range = 1024;
        List<Integer> input = IntStream.range(0, range * 2)
                                       .map(i -> i % range)
                                       .boxed()
                                       .collect(Collectors.toList());

        TestSupport
            .verifyProcessor(sup)
            .jetInstance(jet)
            .input(input)
            .disableSnapshots()
            .disableLogging()
            .disableProgressAssertion()
            .assertOutput(0, (mode, output) -> {
                for (int i = 0; i < range; i++) {
                    assertEquals(Integer.valueOf(2), sinkMap.get(String.valueOf(i)));
                }
                sinkMap.clear();
            });
    }

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<String, Integer> {

        @Override
        public Object process(Entry<String, Integer> entry) {
            Integer val = entry.getValue();
            entry.setValue(val == null ? 1 : val + 1);
            return null;
        }
    }
}
