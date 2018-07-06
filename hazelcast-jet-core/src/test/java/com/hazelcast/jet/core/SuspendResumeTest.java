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
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.exception.JobSuspendRequestedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.Future;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SuspendResumeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int PARALLELISM = 4;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance[] instances;
    private DAG dag;
    private JetConfig config;

    @Before
    public void before() {
        TestProcessors.reset(NODE_COUNT * PARALLELISM);
        instances = new JetInstance[NODE_COUNT];
        config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
        for (int i = 0; i < NODE_COUNT; i++) {
            instances[i] = createJetMember(config);
        }
        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
    }

    @Test
    public void when_suspendAndResume_then_jobResumes() throws Exception {
        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        job.resume();
        assertEqualsEventually(job::getStatus, RUNNING);
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(e -> e instanceof JobSuspendRequestedException));
        }, 5);
    }

    @Test
    public void when_memberAddedAfterResume_then_jobResumesOnAllMembers() throws Exception {
        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        createJetMember(config);
        job.resume();
        assertEqualsEventually(job::getStatus, RUNNING);
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT + 1, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT + 1, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(e -> e instanceof JobSuspendRequestedException));
        }, 5);
    }

    @Test
    public void when_nonCoordinatorDiesAfterSuspend_then_jobResumes() throws Exception {
        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        instances[2].shutdown();
        job.resume();
        assertEqualsEventually(job::getStatus, RUNNING);
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT - 1, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT - 1, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(e -> e instanceof JobSuspendRequestedException));
        }, 5);
    }

    @Test
    public void when_coordinatorDiesAfterSuspend_then_jobResumes() throws Exception {
        // When
        Job job = instances[1].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        instances[0].shutdown();
        job.resume();
        assertEqualsEventually(job::getStatus, RUNNING);
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT - 1, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT - 1, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(e -> e instanceof JobSuspendRequestedException));
        }, 5);
    }

    @Test
    public void when_joinAndThenSuspend_then_joinBlocks() throws Exception {
        Job job = instances[1].newJob(dag);
        StuckProcessor.executionStarted.await();
        // When
        Future future = spawn(job::join);
        sleepSeconds(1); // wait for the join to reach member
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        // Then
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 2);
    }

    @Test
    public void when_suspendAndThenJoin_then_joinBlocks() throws Exception {
        Job job = instances[1].newJob(dag);
        StuckProcessor.executionStarted.await();
        // When
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        Future future = spawn(job::join);
        // Then
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 2);
    }

    @Test
    public void when_joinSuspendedJob_then_waitsAndReturnsAfterResume() throws Exception {
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        // When
        Future future = spawn(job::join);
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 1);
        job.resume();
        assertEqualsEventually(job::getStatus, RUNNING);
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 1);
        StuckProcessor.proceedLatch.countDown();
        assertTrueEventually(() -> assertTrue(future.isDone()), 5);
    }

    @Test
    public void when_cancelSuspendedJob_then_jobCancels() throws Exception {
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        // When
        job.cancel();
        // Then
        assertEqualsEventually(job::getStatus, COMPLETED);

        // check that job resources are deleted
        SnapshotRepository snapshotRepository = new SnapshotRepository(instances[0]);
        JobRepository jobRepository = new JobRepository(instances[0], snapshotRepository);
        assertNull(jobRepository.getJobRecord(job.getId()));
        assertTrue(jobRepository.getJobResult(job.getId()).isSuccessful());
    }
}
