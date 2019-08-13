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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.connector.AsyncHazelcastWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class UpdateMapPTest extends JetTestSupport {

    private JetInstance jet;
    private IMapJet<String, Integer> sinkMap;
    private HazelcastInstance client;

    @Before
    public void setup() {
        jet = createJetMember();
        client = new HazelcastClientProxy((HazelcastClientInstanceImpl) createJetClient().getHazelcastInstance());
        sinkMap = jet.getMap("results");
    }

    @Test
    public void test_localMap() {
        runTest(jet.getHazelcastInstance(), 1, true);
    }

    @Test
    public void test_localMap_highAsync() {
        runTest(jet.getHazelcastInstance(), MAX_PARALLEL_ASYNC_OPS_DEFAULT, true);
    }

    @Test
    public void test_remoteMap() {
        runTest(client, 1, false);
    }

    @Test
    public void test_remoteMap_highAsync() {
        runTest(client, MAX_PARALLEL_ASYNC_OPS_DEFAULT, false);
    }

    private void runTest(HazelcastInstance instance, int asyncLimit, boolean isLocal) {
        SupplierEx<Processor> sup = () -> new UpdateMapP<Integer, String, Integer>(
            instance,
            asyncLimit,
            isLocal,
            sinkMap.getName(),
            Object::toString,
            (prev, next) -> {
                if (prev == null) {
                    return 1;
                }
                return prev + 1;
            });

        int range = 1024;
        int countPerKey = 16;
        List<Integer> input = IntStream.range(0, range * countPerKey)
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
                    assertEquals(Integer.valueOf(countPerKey), sinkMap.get(String.valueOf(i)));
                }
                sinkMap.clear();
            });
    }
}
