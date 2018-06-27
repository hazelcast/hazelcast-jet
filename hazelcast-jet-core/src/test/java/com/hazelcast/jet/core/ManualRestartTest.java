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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class ManualRestartTest extends JetTestSupport {
    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private DAG dag;
    private JetInstance[] instances;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckForeverSourceP::new, NODE_COUNT)));
        instances = createJetMembers(new JetConfig(), NODE_COUNT);
    }

    @Test
    public void when_jobIsRunning_then_itRestarts() {
        testJobRestartWhenJobIsRunning(true);
    }

    @Test
    public void when_autoRestartOnMemberFailureDisabled_then_jobRestarts() {
        testJobRestartWhenJobIsRunning(false);
    }

    private void testJobRestartWhenJobIsRunning(boolean autoRestartOnMemberFailureEnabled) {
        // Given that the job is running
        JetInstance client = createJetClient();
        Job job = client.newJob(dag, new JobConfig().setAutoRestartOnMemberFailure(autoRestartOnMemberFailureEnabled));

        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 10);

        // When the job is restarted after new members join to the cluster
        int newMemberCount = 2;
        for (int i = 0; i < newMemberCount; i++) {
            createJetMember();
        }

        assertTrueAllTheTime(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 3);

        job.restart(false);

        // Then, the job restarts
        int initCount = NODE_COUNT * 2 + newMemberCount;
        assertTrueEventually(() -> assertEquals(initCount, MockPS.initCount.get()), 10);
    }

    @Test
    public void when_jobIsNotBeingExecuted_then_itCannotBeRestarted() {
        // Given that the job execution has not started
        rejectOperationsBetween(instances[0].getHazelcastInstance(), instances[1].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(JetInitDataSerializerHook.INIT_EXECUTION_OP));

        JetInstance client = createJetClient();
        Job job = client.newJob(dag);

        assertTrueEventually(() -> assertSame(job.getStatus(), JobStatus.STARTING), 10);

        // Then, the job cannot restart
        try {
            job.restart(false);
            fail("Restart should have failed");
        } catch (IllegalStateException ignored) { }

        resetPacketFiltersFrom(instances[0].getHazelcastInstance());
    }

    @Test
    public void when_jobIsCompleted_then_itCannotBeRestarted() {
        // Given that the job is completed
        JetInstance client = createJetClient();
        Job job = client.newJob(dag);
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()), 10);
        job.cancel();

        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
        }

        // Then, the job cannot restart
        exception.expect(IllegalStateException.class);
        job.restart(false);
    }
}
