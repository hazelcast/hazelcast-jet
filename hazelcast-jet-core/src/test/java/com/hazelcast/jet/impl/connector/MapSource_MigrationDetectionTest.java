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
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.util.UuidUtil;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.pipeline.Sources.map;
import static com.hazelcast.jet.pipeline.Sources.remoteMap;

@RunWith(HazelcastSerialClassRunner.class)
public class MapSource_MigrationDetectionTest extends JetTestSupport {

    private static CountDownLatch startLatch;
    private static CountDownLatch proceedLatch;

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private List<HazelcastInstance> remoteInstances = new ArrayList<>();

    @After
    public void after() {
        for (HazelcastInstance instance : remoteInstances) {
            instance.shutdown();
        }
    }

    @Test
    public void when_migration_then_detected_local() throws Exception {
        when_migration_then_detected(false);
    }

    @Test
    public void when_migration_then_detected_remote() throws Exception {
        when_migration_then_detected(true);
    }

    private void when_migration_then_detected(boolean remote) throws Exception {
        final JetInstance jobInstance = createJetMember();
        final HazelcastInstance mapInstance;
        final ClientConfig clientConfig;
        Config remoteMemberConfig;
        if (remote) {
            remoteMemberConfig = new Config();
            GroupConfig groupConfig = remoteMemberConfig.getGroupConfig();
            groupConfig.setName(UuidUtil.newUnsecureUuidString());
            mapInstance = Hazelcast.newHazelcastInstance(remoteMemberConfig);
            remoteInstances.add(mapInstance);

            clientConfig = new ClientConfig();
            clientConfig.getGroupConfig().setName(groupConfig.getName());
        } else {
            mapInstance = jobInstance.getHazelcastInstance();
            clientConfig = null;
            remoteMemberConfig = null;
        }

        // populate the map
        IMap<Integer, Integer> m = mapInstance.getMap("map");
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            tmpMap.put(i, i);
        }
        m.putAll(tmpMap);

        startLatch = new CountDownLatch(1);
        proceedLatch = new CountDownLatch(1);

        Pipeline p = Pipeline.create();
        p.drawFrom(remote ? remoteMap(m.getName(), clientConfig) : map(m))
         .setLocalParallelism(1)
         .map(o -> {
             startLatch.countDown();
             proceedLatch.await();
             return o;
         })
         .setLocalParallelism(1)
         .drainTo(Sinks.logger());

        // start the job. The map reader will be blocked thanks to the backpressure from the mapping stage
        Job job = jobInstance.newJob(p, new JobConfig().setAutoScaling(false).setProcessingGuarantee(NONE));

        startLatch.await();
        // create new member, migration will take place
        if (remote) {
            remoteInstances.add(Hazelcast.newHazelcastInstance(remoteMemberConfig));
        } else {
            createJetMember();
        }

        // Then
        // release the latch, map reader should detect the migration and job should fail
        proceedLatch.countDown();

        exception.expectMessage("migration detected");
        job.join();
    }
}
