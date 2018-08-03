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

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.exception.JobSuspendRequestedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
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
        instances[2].getHazelcastInstance().getLifecycleService().terminate();
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
        instances[0].getHazelcastInstance().getLifecycleService().terminate();
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
        try {
            job.join();
            fail("job.join() should have failed");
        } catch (CancellationException ignored) { }
        assertEqualsEventually(job::getStatus, COMPLETED);

        // check that job resources are deleted
        SnapshotRepository snapshotRepository = new SnapshotRepository(instances[0]);
        JobRepository jobRepository = new JobRepository(instances[0], snapshotRepository);
        assertTrueEventually(() -> {
            assertNull("JobRecord", jobRepository.getJobRecord(job.getId()));
            JobResult jobResult = jobRepository.getJobResult(job.getId());
            assertInstanceOf(CancellationException.class, jobResult.getFailure());
            assertFalse("Job result successful", jobResult.isSuccessful());
        });
    }

    @Test
    public void when_restartSuspendedJob_then_fail() throws Exception {
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        // Then
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot RESTART_GRACEFUL, job status is SUSPENDED");
        // When
        job.restart();
    }

    @Test
    public void when_suspendSuspendedJob_then_fail() throws Exception {
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        // Then
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot SUSPEND_GRACEFUL, job status is SUSPENDED");
        // When
        job.suspend();
    }

    @Test
    public void when_jobSuspendedAndCoordinatorShutDown_then_jobStaysSuspended() throws Exception {
        when_jobSuspendedAndCoordinatorGone_then_jobStaysSuspended(true);
    }

    @Test
    public void when_jobSuspendedAndCoordinatorTerminated_then_jobStaysSuspended() throws Exception {
        when_jobSuspendedAndCoordinatorGone_then_jobStaysSuspended(false);
    }

    private void when_jobSuspendedAndCoordinatorGone_then_jobStaysSuspended(boolean graceful) throws Exception {
        assertTrue(((ClusterService) instances[0].getCluster()).isMaster());
        Job job = instances[1].newJob(dag);
        StuckProcessor.executionStarted.await();
        job.suspend();
        assertEqualsEventually(job::getStatus, SUSPENDED);
        if (graceful) {
            instances[0].shutdown();
        } else {
            instances[0].getHazelcastInstance().getLifecycleService().terminate();
        }
        assertTrueAllTheTime(() -> assertEquals(SUSPENDED, job.getStatus()), 10);
    }
}
