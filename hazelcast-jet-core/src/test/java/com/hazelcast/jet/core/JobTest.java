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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.Identity;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class JobTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;

    @Before
    public void setup() {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        instance1 = createJetMember(config);
        instance2 = createJetMember(config);
    }

    @Test
    public void when_jobIsSubmittedFromNonMaster_then_jobStatusShouldBeStarting() {
        testJobStatusDuringStart(instance2);
    }

    @Test
    public void when_jobIsSubmittedFromClient_then_jobStatusShouldBeStarting() {
        testJobStatusDuringStart(createJetClient());
    }

    private void testJobStatusDuringStart(JetInstance submitter) {
        PSThatWaitsOnInit.initLatch = new CountDownLatch(1);
        DAG dag = new DAG().vertex(new Vertex("test", new PSThatWaitsOnInit(Identity::new)));

        // When
        Job job = submitter.newJob(dag);
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_RUNNING || status == STARTING);

        PSThatWaitsOnInit.initLatch.countDown();

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting_fromNonMaster() {
        when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting(instance2);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting_fromClient() {
        when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting(createJetClient());
    }

    private void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting(JetInstance submitter) {
        PSThatWaitsOnInit.initLatch = new CountDownLatch(1);
        DAG dag = new DAG().vertex(new Vertex("test", new PSThatWaitsOnInit(Identity::new)));

        // When
        Job job = submitter.newJobIfAbsent(dag, new JobConfig());
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_RUNNING || status == STARTING);

        PSThatWaitsOnInit.initLatch.countDown();

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobIsCancelled_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // Then
        job.cancel();
        joinAndExpectCancellation(job);

        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(job, FAILED);
    }

    @Test
    public void when_jobIsFailed_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS((SupplierEx<Processor>)
                () -> new MockP().setCompleteError(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);

        // Then
        try {
            job.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, job.getStatus());
        }
    }

    @Test
    public void when_jobIsSubmitted_then_trackedJobCanQueryJobStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        submittedJob.cancel();
        joinAndExpectCancellation(submittedJob);
    }

    @Test
    public void when_jobIsSubmittedWithNewJobIfAbsent_then_trackedJobCanQueryJobStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJobIfAbsent(dag, new JobConfig());
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        submittedJob.cancel();
        joinAndExpectCancellation(submittedJob);
    }

    @Test
    public void when_namedJobIsSubmittedWithNewJobIfAbsent_then_trackedJobCanQueryJobStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job submittedJob = instance1.newJobIfAbsent(dag, config);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        submittedJob.cancel();
        joinAndExpectCancellation(submittedJob);
    }

    @Test
    public void when_jobIsCompleted_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        instance1.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        NoOutputSourceP.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsCancelled_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        submittedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(trackedJob, FAILED);
    }

    @Test
    public void when_jobIsFailed_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS((SupplierEx<Processor>)
                () -> new MockP().setCompleteError(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        instance1.newJob(dag);

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        try {
            trackedJob.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, trackedJob.getStatus());
        }
    }

    @Test
    public void when_trackedJobCancels_then_jobCompletes() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        Job submittedJob = instance1.newJob(dag);

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // When
        trackedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);
        joinAndExpectCancellation(submittedJob);

        NoOutputSourceP.proceedLatch.countDown();

        assertJobStatusEventually(trackedJob, FAILED);
        assertJobStatusEventually(submittedJob, FAILED);
    }

    @Test
    public void when_jobIsCompleted_then_trackedJobCanQueryJobResultFromClient() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        instance1.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        JetInstance client = createJetClient();

        Collection<Job> trackedJobs = client.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        NoOutputSourceP.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByName() throws InterruptedException {
        testGetJobByNameWhenJobIsRunning(instance2);
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByNameFromClient() throws InterruptedException {
        testGetJobByNameWhenJobIsRunning(createJetClient());
    }

    private void testGetJobByNameWhenJobIsRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        assertEquals(jobName, job.getName());
        NoOutputSourceP.executionStarted.await();

        // Then
        Job trackedJob = instance.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertJobStatusEventually(trackedJob, RUNNING);

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedById() throws InterruptedException {
        testGetJobByIdWhenJobIsRunning(instance1);
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByIdFromClient() throws InterruptedException {
        testGetJobByIdWhenJobIsRunning(createJetClient());
    }

    private void testGetJobByIdWhenJobIsRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // Then
        Job trackedJob = instance.getJob(job.getId());

        assertNotNull(trackedJob);
        assertEquals(job.getId(), trackedJob.getId());
        assertJobStatusEventually(trackedJob, RUNNING);

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance1.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedById() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance1.getJob(job.getId());

        assertNotNull(trackedJob);
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsQueriedByInvalidId_then_noJobIsReturned() {
        assertNull(instance1.getJob(0));
    }

    @Test
    public void when_jobIsQueriedByInvalidIdFromClient_then_noJobIsReturned() {
        assertNull(createJetClient().getJob(0));
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedSubmitJoinsToExistingJob_member() {
        when_namedJobIsRunning_then_newNamedSubmitJoinsToExistingJob(instance1);
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedSubmitJoinsToExistingJob_client() {
        when_namedJobIsRunning_then_newNamedSubmitJoinsToExistingJob(createJetClient());
    }

    private void when_namedJobIsRunning_then_newNamedSubmitJoinsToExistingJob(JetInstance instance) {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");
        Job job1 = instance.newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job1.getStatus()));

        // When
        Job job2 = instance.newJobIfAbsent(dag, config);

        // Then
        assertEquals(job1.getId(), job2.getId());
        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void stressTest_parallelNamedJobSubmission_member() throws Exception {
        stressTest_parallelNamedJobSubmission(instance1);
    }

    @Test
    public void stressTest_parallelNamedJobSubmission_client() throws Exception {
        stressTest_parallelNamedJobSubmission(createJetClient());
    }

    private void stressTest_parallelNamedJobSubmission(JetInstance instance) throws Exception {
        final int nThreads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        try {
            for (int round = 0; round < 10; round++) {
                DAG dag = new DAG().vertex(new Vertex("test" + round, new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
                System.out.println("Starting round " + round);
                JobConfig config = new JobConfig().setName("job" + round);
                List<Future<Job>> futures = new ArrayList<>();
                for (int i = 0; i < nThreads; i++) {
                    futures.add(executor.submit(() -> instance.newJobIfAbsent(dag, config)));
                }
                for (int i = 1; i < nThreads; i++) {
                    assertEquals(futures.get(0).get().getId(), futures.get(i).get().getId());
                }
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedJobFails_member() {
        when_namedJobIsRunning_then_newNamedJobFails(instance1);
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedJobFails_client() {
        when_namedJobIsRunning_then_newNamedJobFails(createJetClient());
    }

    private void when_namedJobIsRunning_then_newNamedJobFails(JetInstance instance) {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance.newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job1.getStatus()));

        // Then
        expectedException.expect(JobAlreadyExistsException.class);
        instance.newJob(dag, config);
    }

    @Test
    public void when_namedJobHasCompletedAndAnotherWasSubmitted_then_runningOneIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance1.newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job2.getStatus()));

        // Then
        Job trackedJob = instance1.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertNotEquals(job1.getId(), trackedJob.getId());
        assertEquals(job2.getId(), trackedJob.getId());
        assertEquals(RUNNING, trackedJob.getStatus());
    }

    @Test
    public void when_namedJobHasCompletedAndAnotherWasSubmitted_then_bothAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance1.newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job2.getStatus()));

        // Then
        List<Job> trackedJobs = instance1.getJobs(jobName);

        assertEquals(2, trackedJobs.size());

        Job trackedJob1 = trackedJobs.get(0);
        Job trackedJob2 = trackedJobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals(RUNNING, trackedJob1.getStatus());

        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals(COMPLETED, trackedJob2.getStatus());
    }

    @Test
    public void when_jobConfigChanged_then_doesNotAffectSubmittedJob() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        Job job1 = instance1.newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);

        // When
        config.setName("job2");

        // Then
        Job job2 = instance1.newJob(dag, config);
        assertJobStatusEventually(job2, RUNNING);
    }

    @Test
    public void when_jobsAreCompleted_then_theyAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        sleepAtLeastMillis(1);

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance1.newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job2.join();

        // Then
        List<Job> jobs = instance1.getJobs("job1");
        assertEquals(2, jobs.size());

        Job trackedJob1 = jobs.get(0);
        Job trackedJob2 = jobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals("job1", trackedJob1.getName());
        assertEquals(COMPLETED, trackedJob1.getStatus());
        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals("job1", trackedJob2.getName());
        assertEquals(COMPLETED, trackedJob2.getStatus());
    }

    @Test
    public void when_suspendedNamedJob_then_newJobIfAbsentWithEqualNameJoinsIt() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);

        // Then
        Job job2 = instance2.newJobIfAbsent(dag, config);
        assertEquals(job1.getId(), job2.getId());
        assertEquals(job2.getStatus(), SUSPENDED);
    }

    @Test
    public void when_suspendedNamedJob_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);

        // Then
        expectedException.expect(JobAlreadyExistsException.class);
        instance2.newJob(dag, config);
    }

    @Test
    public void when_suspendedJobScannedOnNewMaster_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);
        // gracefully shutdown the master
        instance1.shutdown();

        // Then
        expectedException.expect(JobAlreadyExistsException.class);
        instance2.newJob(dag, config);
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried() throws InterruptedException {
        testJobSubmissionTimeWhenJobIsRunning(instance1);
    }

    @Test
    public void when_jobIsRunning_then_jobSubmissionTimeIsQueriedFromClient() throws InterruptedException {
        testJobSubmissionTimeWhenJobIsRunning(createJetClient());
    }

    private void testJobSubmissionTimeWhenJobIsRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        NoOutputSourceP.executionStarted.await();
        Job trackedJob = instance.getJob("job1");

        // Then
        assertNotNull(trackedJob);
        assertNotEquals(0, job.getSubmissionTime());
        assertNotEquals(0, trackedJob.getSubmissionTime());
        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_jobSubmissionTimeIsQueried() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        Job trackedJob = instance1.getJob("job1");

        // Then
        assertNotNull(trackedJob);
        assertNotEquals(0, job.getSubmissionTime());
        assertNotEquals(0, trackedJob.getSubmissionTime());
    }

    private void joinAndExpectCancellation(Job job) {
        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
        }
    }

    private static final class PSThatWaitsOnInit implements ProcessorSupplier {

        public static volatile CountDownLatch initLatch;
        private final SupplierEx<Processor> supplier;

        PSThatWaitsOnInit(SupplierEx<Processor> supplier) {
            this.supplier = supplier;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            initLatch.await();
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());

        }
    }
}
