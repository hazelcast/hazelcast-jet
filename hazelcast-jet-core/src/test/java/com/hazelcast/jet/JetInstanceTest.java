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

package com.hazelcast.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

@Category(NightlyTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JetInstanceTest extends JetTestSupport {

    private static final String UNABLE_TO_CONNECT_MESSAGE = "Unable to connect";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        TestProcessors.reset(1);
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
        Jet.shutdownAll();
    }

    @Test
    public void when_twoJetInstancesCreated_then_clusterOfTwoShouldBeFormed() {
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        assertEquals(2, instance1.getCluster().getMembers().size());
    }

    @Test
    public void when_twoJetAndTwoHzInstancesCreated_then_twoClustersOfTwoShouldBeFormed() {
        JetInstance jetInstance1 = Jet.newJetInstance();
        JetInstance jetInstance2 = Jet.newJetInstance();

        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance();

        assertEquals(2, jetInstance1.getCluster().getMembers().size());
        assertEquals(2, hazelcastInstance1.getCluster().getMembers().size());
    }

    @Test
    public void when_jetClientCreated_then_connectsToJetCluster() {
        JetInstance jetInstance1 = Jet.newJetInstance();
        JetInstance jetInstance2 = Jet.newJetInstance();

        JetInstance client = Jet.newJetClient(new JetClientConfig());

        assertEquals(2, client.getHazelcastInstance().getCluster().getMembers().size());
    }

    @Test
    public void when_jetClientCreated_then_doesNotConnectToHazelcastCluster() {
        Hazelcast.newHazelcastInstance();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(UNABLE_TO_CONNECT_MESSAGE);
        Jet.newJetClient();
    }

    @Test
    public void when_hazelcastClientCreated_then_doesNotConnectToJetCluster() {
        Jet.newJetInstance();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(UNABLE_TO_CONNECT_MESSAGE);
        HazelcastClient.newHazelcastClient();
    }

    @Test
    public void smokeTest_exportedStateManagement() {
        JetInstance instance = Jet.newJetInstance();
        DAG dag = new DAG();
        dag.newVertex("p", () -> new StuckProcessor());
        Job job = instance.newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        job.exportState("state1");
        instance.getHazelcastInstance().getDistributedObjects().forEach(System.out::println);
        assertEquals(singleton("state1"), new HashSet<>(instance.getExportedStateNames()));
        job.exportState("state2");
        assertEquals(new HashSet<>(asList("state1", "state2")), new HashSet<>(instance.getExportedStateNames()));
        instance.deleteExportedState("state1");
        assertEquals(singleton("state2"), new HashSet<>(instance.getExportedStateNames()));
        job.cancel();
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        assertEquals(singleton("state2"), new HashSet<>(instance.getExportedStateNames()));
        instance.deleteExportedState("state1");
        assertEquals(emptySet(), new HashSet<>(instance.getExportedStateNames()));
    }
}
