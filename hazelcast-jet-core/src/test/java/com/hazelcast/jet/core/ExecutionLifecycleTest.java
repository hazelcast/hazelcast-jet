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

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPMS;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.assertExceptionInCauses;
import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.core.processor.Processors.nonCooperativeP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.impl.execution.SnapshotContext.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionLifecycleTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 4;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance;

    @Before
    public void setup() {
        MockPMS.initCalled.set(false);
        MockPMS.closeCalled.set(false);
        MockPMS.receivedCloseError.set(null);

        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        MockP.initCount.set(0);
        MockP.closeCount.set(0);
        MockP.receivedCloseErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        instance = createJetMember(config);
        createJetMember(config);
    }

    @Test
    public void when_jobCompletesSuccessfully_then_closeCalled() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT))));

        // When
        Job job = instance.newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_pmsInitThrows_then_jobFails() {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT)).setInitError(e)));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_pmsGetThrows_then_jobFails() {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT)).setGetError(e)));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_pmsCloseThrows_then_jobSucceeds() {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT)).setCloseError(e)));

        // When
        Job job = instance.newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_psInitThrows_then_jobFails() {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT).setInitError(e))));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPsClosedWithError(e);
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_psGetThrows_then_jobFails() {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT).setGetError(e))));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPsClosedWithError(e);
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_psGetOnOtherNodeThrows_then_jobFails() throws Throwable {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        final int localPort = instance.getCluster().getLocalMember().getAddress().getPort();

        DAG dag = new DAG().vertex(new Vertex("faulty",
                ProcessorMetaSupplier.of(
                        (Address address) -> ProcessorSupplier.of(
                                address.getPort() == localPort ? noopP() : () -> {
                                    throw e;
                                })
                )));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_psCloseThrows_then_jobSucceeds() {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(MockP::new, NODE_COUNT).setCloseError(e))));

        // When
        Job job = instance.newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_processorInitThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setInitError(e), NODE_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPClosedWithError(e, true);
        assertPsClosedWithError(e);
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_processorProcessThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex source = dag.newVertex("source", ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setProcessError(e), NODE_COUNT)));
        dag.edge(between(source, process));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPClosedWithError(e, false);
        assertPsClosedWithError(e);
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_processorCooperativeCompleteThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setCompleteError(e), NODE_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPClosedWithError(e, false);
        assertPsClosedWithError(e);
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_processorNonCooperativeCompleteThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        dag.newVertex("faulty", nonCooperativeP(
                new MockPMS(() -> new MockPS(() -> new MockP().setCompleteError(e), NODE_COUNT))));

        // When
        Job job = runJobExpectFailure(dag, e);

        // Then
        assertPClosedWithError(e, false);
        assertPsClosedWithError(e);
        assertPmsClosedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_processorCloseThrows_then_jobSucceeds() {
        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setCloseError(e), NODE_COUNT)));

        // When
        Job job = instance.newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_executionCancelled_then_jobCompletedWithCancellationException() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(StuckProcessor::new, NODE_COUNT))));

        // When
        Job job = instance.newJob(dag);
        try {
            StuckProcessor.executionStarted.await();
            job.cancel();
            job.join();
            fail("Job execution should fail");
        } catch (CancellationException ignored) {
        }

        assertTrueEventually(() -> {
            assertJobFailed(job, new CancellationException());
            assertPsClosedWithError(new CancellationException());
            assertPmsClosedWithError(new CancellationException());
        });
    }

    @Test
    public void when_executionCancelledBeforeStart_then_jobFutureIsCancelledOnExecute() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPS(StuckProcessor::new, NODE_COUNT)));

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance.getHazelcastInstance());
        Address localAddress = nodeEngineImpl.getThisAddress();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngineImpl.getClusterService();
        MembersView membersView = clusterService.getMembershipManager().getMembersView();
        int memberListVersion = membersView.getVersion();

        JetService jetService = getJetService(instance);
        final Map<MemberInfo, ExecutionPlan> executionPlans =
                ExecutionPlanBuilder.createExecutionPlans(nodeEngineImpl, membersView, dag, new JobConfig(),
                        NO_SNAPSHOT);
        ExecutionPlan executionPlan = executionPlans.get(membersView.getMember(localAddress));
        long jobId = 0;
        long executionId = 1;

        Set<MemberInfo> participants = new HashSet<>(membersView.getMembers());
        jetService.getJobExecutionService().initExecution(
                jobId, executionId, localAddress, memberListVersion, participants, executionPlan
        );

        ExecutionContext executionContext = jetService.getJobExecutionService().getExecutionContext(executionId);
        executionContext.cancelExecution();

        // When
        CompletableFuture<Void> future = executionContext.beginExecution();

        // Then
        expectedException.expect(CancellationException.class);
        future.join();
    }

    @Test
    public void when_jobCancelled_then_psCloseNotCalledBeforeTaskletsDone() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPS(() -> new StuckProcessor(10_000), NODE_COUNT)));

        // When
        Job job = instance.newJob(dag);

        assertOpenEventually(StuckProcessor.executionStarted);

        // Then
        job.cancel();

        assertTrueEventually(() -> assertEquals(JobStatus.COMPLETING, job.getStatus()), 3);

        assertTrueFiveSeconds(() -> {
            assertEquals(JobStatus.COMPLETING, job.getStatus());
            assertEquals("PS.close called before execution finished", 0, MockPS.closeCount.get());
        });

        StuckProcessor.proceedLatch.countDown();

        expectedException.expect(CancellationException.class);
        job.join();

        assertEquals("PS.close not called after execution finished", NODE_COUNT, MockPS.closeCount.get());
    }

    @Test
    public void when_deserializationOnMembersFails_then_jobSubmissionFails__member() throws Throwable {
        when_deserializationOnMembersFails_then_jobSubmissionFails(createJetMember());
    }

    @Test
    public void when_deserializationOnMembersFails_then_jobSubmissionFails__client() throws Throwable {
        createJetMember();
        when_deserializationOnMembersFails_then_jobSubmissionFails(createJetClient());
    }

    public void when_deserializationOnMembersFails_then_jobSubmissionFails(JetInstance instance) throws Throwable {
        // Given
        DAG dag = new DAG();
        // this is designed to fail when member deserializes the execution plan while executing
        // the InitOperation
        dag.newVertex("faulty", (ProcessorMetaSupplier) addresses -> address -> new NotDeserializableProcessorSupplier());

        // Then
        expectedException.expect(HazelcastSerializationException.class);
        expectedException.expectMessage("fake.Class");

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_deserializationOnMasterFails_then_jobSubmissionFails_member() throws Throwable {
        when_deserializationOnMasterFails_then_jobSubmissionFails(createJetMember());
    }

    @Test
    public void when_deserializationOnMasterFails_then_jobSubmissionFails_client() throws Throwable {
        createJetMember();
        when_deserializationOnMasterFails_then_jobSubmissionFails(createJetClient());
    }

    public void when_deserializationOnMasterFails_then_jobSubmissionFails(JetInstance instance) throws Throwable {
        // Given
        DAG dag = new DAG();
        // this is designed to fail when the master member deserializes the DAG
        dag.newVertex("faulty", new NotDeserializableProcessorMetaSupplier());

        // Then
        expectedException.expect(HazelcastSerializationException.class);
        expectedException.expectMessage("fake.Class");

        // When
        executeAndPeel(instance.newJob(dag));
    }

    public Job runJobExpectFailure(@Nonnull DAG dag, @Nonnull RuntimeException expectedException) {
        Job job = null;
        try {
            job = instance.newJob(dag);
            job.join();
            fail("Job execution should have failed");
        } catch (Exception actual) {
            Throwable cause = peel(actual);
            assertContains(cause.getMessage(), expectedException.getMessage());
        }
        return job;
    }

    private void assertPmsClosedWithoutError() {
        assertTrue("initCalled", MockPMS.initCalled.get());
        assertTrue("closeCalled", MockPMS.closeCalled.get());
        assertNull("receivedCloseError", MockPMS.receivedCloseError.get());
    }

    private void assertPmsClosedWithError(RuntimeException e) {
        assertTrue("initCalled", MockPMS.initCalled.get());
        assertTrue("closeCalled", MockPMS.closeCalled.get());
        assertExceptionInCauses(e, MockPMS.receivedCloseError.get());
    }

    private void assertPsClosedWithoutError() {
        assertEquals(NODE_COUNT, MockPS.initCount.get());
        assertEquals(NODE_COUNT, MockPS.closeCount.get());
        assertEquals(0, MockPS.receivedCloseErrors.size());
    }

    private void assertPsClosedWithError(Throwable e) {
        assertEquals(NODE_COUNT, MockPS.initCount.get());
        assertEquals(NODE_COUNT, MockPS.closeCount.get());
        assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertExceptionInCauses(e, MockPS.receivedCloseErrors.get(i));
        }
    }

    private void assertPClosedWithoutError() {
        assertEquals(TOTAL_PARALLELISM, MockP.initCount.get());
        assertEquals(TOTAL_PARALLELISM, MockP.closeCount.get());
        assertEquals(0, MockP.receivedCloseErrors.size());
    }

    private void assertPClosedWithError(Throwable e, boolean initExpectedToFail) {
        // can't assert initCount here, init() is not called on next processor after first processor fails
        if (!initExpectedToFail) {
            assertEquals(TOTAL_PARALLELISM, MockP.initCount.get());
        }
        assertEquals(TOTAL_PARALLELISM, MockP.closeCount.get());
        assertEquals(TOTAL_PARALLELISM, MockP.receivedCloseErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertExceptionInCauses(e, MockP.receivedCloseErrors.get(i));
        }
    }

    private void assertJobSucceeded(Job job) {
        JobResult jobResult = getJobResult(job);
        assertTrue(jobResult.isSuccessful());
        assertNull(jobResult.getFailure());
    }

    private void assertJobFailed(Job job, Throwable e) {
        JobResult jobResult = getJobResult(job);
        assertFalse("jobResult.isSuccessful", jobResult.isSuccessful());
        assertExceptionInCauses(e, jobResult.getFailure());
        JobStatus expectedStatus = e instanceof CancellationException ? JobStatus.COMPLETED : JobStatus.FAILED;
        assertEquals("jobStatus", expectedStatus, job.getStatus());
    }

    private JobResult getJobResult(Job job) {
        JetService jetService = getJetService(instance);
        assertNull(jetService.getJobRepository().getJobRecord(job.getId()));
        JobResult jobResult = jetService.getJobRepository().getJobResult(job.getId());
        assertNotNull(jobResult);
        return jobResult;
    }

    private static class NotDeserializableProcessorSupplier implements ProcessorSupplier {
        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            throw new UnsupportedOperationException("should not get here");
        }

        private void readObject(java.io.ObjectInputStream stream) throws ClassNotFoundException {
            // simulate deserialization failure
            throw new ClassNotFoundException("fake.Class");
        }
    }

    private static class NotDeserializableProcessorMetaSupplier implements ProcessorMetaSupplier {
        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            throw new UnsupportedOperationException("should not get here");
        }

        private void readObject(java.io.ObjectInputStream stream) throws ClassNotFoundException {
            // simulate deserialization failure
            throw new ClassNotFoundException("fake.Class");
        }
    }
}
