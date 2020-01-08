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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.pipeline.Sources.map;
import static com.hazelcast.jet.pipeline.Sources.remoteMap;
import static java.util.stream.Collectors.toList;

@RunWith(HazelcastSerialClassRunner.class)
public class MapSource_MigrationTest extends JetTestSupport {

    private static final String MAP_NAME = "map";
    private static final int NO_ITEMS = 100_000;

    private static AtomicInteger processedCount;
    private static CountDownLatch startLatch;
    private static CountDownLatch proceedLatch;

    private List<HazelcastInstance> remoteInstances = new ArrayList<>();
    private JetInstance jet;

    @Before
    public void setup() {
        jet = createJetMember();
    }

    @After
    public void after() {
        for (HazelcastInstance instance : remoteInstances) {
            instance.shutdown();
        }
    }

    @Test
    public void when_migration_then_stable_iteration_local() throws Exception {
        populateMap(jet.getMap(MAP_NAME));
        test(null, this::createJetMember);
    }

    @Test
    public void when_migration_then_stable_iteration_remote() throws Exception {
        Config config = new Config().setClusterName(UuidUtil.newUnsecureUuidString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        remoteInstances.add(hz);

        ClientConfig clientConfig = new ClientConfig().setClusterName(config.getClusterName());

        populateMap(hz.getMap(MAP_NAME));

        test(clientConfig, () -> remoteInstances.add(Hazelcast.newHazelcastInstance(config)));
    }

    private void test(ClientConfig clientConfig, Runnable newInstance) throws InterruptedException {
        Pipeline p = Pipeline.create();
        p.readFrom(clientConfig == null ? map(MAP_NAME) : remoteMap(MAP_NAME, clientConfig))
          .setLocalParallelism(1)
          .map(o -> {
             // process first 100 items, then wait for migration
             int count = processedCount.incrementAndGet();
             if (count == 1024) {
                 // signal to start new node
                 startLatch.countDown();
             } else if (count > 1024) {
                 // wait for migration to complete
                 proceedLatch.await();
             }
             return (Integer) o.getKey();
         })
          .setLocalParallelism(1)
          .writeTo(AssertionSinks.assertAnyOrder(IntStream.range(0, NO_ITEMS).boxed().collect(toList())));

        processedCount = new AtomicInteger();
        startLatch = new CountDownLatch(1);
        proceedLatch = new CountDownLatch(1);

        Job job = jet.newJob(p, new JobConfig().setAutoScaling(false));

        // start the job. The map reader will be blocked thanks to the backpressure from the mapping stage
        startLatch.await();
        // create new member, migration will take place
        newInstance.run();
        // release the latch, the rest of the items will be processed after the migration
        proceedLatch.countDown();

        job.join();
    }

    private void populateMap(IMap<Integer, Integer> map) {
        // populate the map
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < NO_ITEMS; i++) {
            tmpMap.put(i, i);
        }
        map.putAll(tmpMap);
    }

}
