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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.pipeline.Sources.map;
import static com.hazelcast.jet.pipeline.Sources.remoteMap;

public class ReadWithPartitionIteratorP_MigrationDetectionTest extends JetTestSupport {

    private static CountDownLatch latch;

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
    public void when_migration_then_detected_local() {
        when_migration_then_detected(false);
    }

    @Test
    public void when_migration_then_detected_remote() {
        when_migration_then_detected(true);
    }

    private void when_migration_then_detected(boolean remote) {
        final JetInstance jobInstance = createJetMember();
        final HazelcastInstance mapInstance;
        final ClientConfig clientConfig;
        Config remoteMemberConfig;
        if (remote) {
            remoteMemberConfig = new Config();
            GroupConfig groupConfig = remoteMemberConfig.getGroupConfig();
            groupConfig.setName("remote-cluster");
            groupConfig.setPassword("remote-cluster");
            mapInstance = Hazelcast.newHazelcastInstance(remoteMemberConfig);
            remoteInstances.add(mapInstance);

            clientConfig = new ClientConfig();
            clientConfig.getGroupConfig().setName(groupConfig.getName());
            clientConfig.getGroupConfig().setPassword(groupConfig.getPassword());
        } else {
            mapInstance = jobInstance.getHazelcastInstance();
            clientConfig = null;
            remoteMemberConfig = null;
        }

        // populate the map
        IMap m = mapInstance.getMap("map");
        Map tmpMap = new HashMap();
        for (int i = 0; i < 10000; i++) {
            tmpMap.put(i, i);
        }
        m.putAll(tmpMap);

        Pipeline p = Pipeline.create();
        p.drawFrom(remote ? remoteMap(m.getName(), clientConfig) : map(m))
         .setLocalParallelism(1)
         .map(o -> {
             latch.await();
             return o;
         })
         .setLocalParallelism(1)
         .drainTo(Sinks.logger());

        // start the job. The map reader will be blocked thanks to the backpressure from the mapping stage
        latch = new CountDownLatch(1);
        Job job = jobInstance.newJob(p);

        // create new member, migration will take place
        if (remote) {
            remoteInstances.add(Hazelcast.newHazelcastInstance(remoteMemberConfig));
        } else {
            createJetMember();
        }

        // Then
        // release the latch, map reader should detect the migration and job should fail
        latch.countDown();

        exception.expect(Exception.class);
        exception.expectMessage("migration detected");
        job.join();
    }
}
