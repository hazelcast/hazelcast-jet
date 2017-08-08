/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.TestProcessors.Identity;
import com.hazelcast.jet.TestProcessors.ProcessorThatFailsInComplete;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.hazelcast.jet.TestUtil.assertExceptionInCauses;
import static com.hazelcast.jet.TestUtil.getJetService;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionLifecycleTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 4;

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance;
    private JetTestInstanceFactory factory;


    @Before
    public void setup() {
        MockSupplier.completeCount.set(0);
        MockSupplier.initCount.set(0);
        MockSupplier.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        instance = factory.newMember(config);
        factory.newMember(config);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void when_procSupplierInit_then_completeCalled() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(Identity::new)));

        // When
        Job job = instance.newJob(dag);
        job.join();

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());
        assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
        assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertNull(MockSupplier.completeErrors.get(i));
        }

        JetService jetService = getJetService(instance);
        assertNull(jetService.getJobRepository().getJob(job.getJobId()));
        JobResult jobResult = jetService.getJobCoordinationService().getJobResult(job.getJobId());
        assertNotNull(jobResult);
        assertTrue(jobResult.isSuccessful());
        assertNull(jobResult.getFailure());
    }

    @Test
    public void when_procSupplierFailsOnInit_then_completeCalledWithError() throws Throwable {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(e, Identity::new)));

        // When
        Job job = null;
        try {
            job = instance.newJob(dag);
            job.join();
            fail("Job execution should fail");
        } catch (Exception expected) {
            Throwable cause = peel(expected);
            assertEquals(e.getMessage(), cause.getMessage());
        }

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());

        assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
        assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertEquals(e.getMessage(), MockSupplier.completeErrors.get(i).getMessage());
        }

        assertNotNull(job);
        JetService jetService = getJetService(instance);
        assertNull(jetService.getJobRepository().getJob(job.getJobId()));
        JobResult jobResult = jetService.getJobCoordinationService().getJobResult(job.getJobId());
        assertNotNull(jobResult);
        assertFalse(jobResult.isSuccessful());
        assertTrue(jobResult.getFailure() instanceof RuntimeException);
    }

    @Test
    public void when_executionFails_then_completeCalledWithError() throws Throwable {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        String vertexName = "test";
        DAG dag = new DAG().vertex(new Vertex(vertexName, new MockSupplier(() -> new ProcessorThatFailsInComplete(e))));

        // When
        Job job = null;
        try {
            job = instance.newJob(dag);
            job.join();
            fail("Job execution should fail");
        } catch (Exception expected) {
            assertExceptionInCauses(e, expected);
            String expectedMessage = "vertex=" + vertexName + "";
            assertTrue("Error message does not contain vertex name.\nExpected: " + expectedMessage
                            + "\nActual: " + expected,
                    expected.getMessage() != null && expected.getMessage().contains(expectedMessage));
        }

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());
        assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
        assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());

        for (Throwable caught : MockSupplier.completeErrors) {
            assertExceptionInCauses(e, caught);
        }

        assertNotNull(job);
        JetService jetService = getJetService(instance);
        assertNull(jetService.getJobRepository().getJob(job.getJobId()));
        JobResult jobResult = jetService.getJobCoordinationService().getJobResult(job.getJobId());
        assertNotNull(jobResult);
        assertFalse(jobResult.isSuccessful());
        assertTrue(jobResult.getFailure() instanceof RuntimeException);
        assertEquals(JobStatus.FAILED, job.getJobStatus());
    }

    @Test
    public void when_executionCancelled_then_completeCalledAfterExecutionDone() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new)));

        // When
        try {
            Job job = instance.newJob(dag);
            StuckProcessor.executionStarted.await();
            job.cancel();
            job.join();
            fail("Job execution should fail");
        } catch (CancellationException ignored) {
        }

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());
        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, MockSupplier.completeCount.get());
            }
        });

        StuckProcessor.proceedLatch.countDown();

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
            assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());
            for (int i = 0; i < NODE_COUNT; i++) {
                assertInstanceOf(CancellationException.class, MockSupplier.completeErrors.get(i));
            }
        });
    }

    @Test
    public void when_executionCancelledBeforeStart_then_jobFutureIsCancelledOnExecute() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new)));

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance.getHazelcastInstance());
        Address localAddress = nodeEngineImpl.getThisAddress();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngineImpl.getClusterService();
        MembersView membersView = clusterService.getMembershipManager().getMembersView();
        int memberListVersion = membersView.getVersion();

        JetService jetService = getJetService(instance);
        final Map<MemberInfo, ExecutionPlan> executionPlans =
                jetService.getJobCoordinationService().createExecutionPlans(membersView, dag);
        ExecutionPlan executionPlan = executionPlans.get(membersView.getMember(localAddress));
        long jobId = 0;
        long executionId = 1;

        jetService.initExecution(jobId, executionId, localAddress, memberListVersion,
                new HashSet<>(membersView.getMembers()), executionPlan);

        ExecutionContext executionContext = jetService.getJobExecutionService().getExecutionContext(executionId);
        executionContext.cancel();

        // When
        final AtomicReference<Object> result = new AtomicReference<>();

        executionContext.execute(stage ->
                stage.whenComplete((aVoid, throwable) -> result.compareAndSet(null, throwable)));

        // Then
        assertTrue(result.get() instanceof CancellationException);
    }

    @Test
    public void when_completeStepThrowsException_then_jobStillSucceeds() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new FailingOnCompleteSupplier(Identity::new)));

        // When
        Job job = instance.newJob(dag);

        // Then
        job.join();
        assertEquals(JobStatus.COMPLETED, job.getJobStatus());
    }

    private static class FailingOnCompleteSupplier implements ProcessorSupplier {

        private final DistributedSupplier<Processor> supplier;

        FailingOnCompleteSupplier(DistributedSupplier<Processor> supplier) {
            this.supplier = supplier;
        }

        @Override
        public void init(@Nonnull Context context) {

        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            throw new ExpectedRuntimeException();
        }
    }

    private static class MockSupplier implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger completeCount = new AtomicInteger();
        static List<Throwable> completeErrors = new CopyOnWriteArrayList<>();

        private final RuntimeException initError;
        private final DistributedSupplier<Processor> supplier;

        private boolean initCalled;

        MockSupplier(DistributedSupplier<Processor> supplier) {
            this(null, supplier);
        }

        MockSupplier(RuntimeException initError, DistributedSupplier<Processor> supplier) {
            this.initError = initError;
            this.supplier = supplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            initCalled = true;
            initCount.incrementAndGet();

            if (initError != null) {
                throw initError;
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            completeErrors.add(error);
            completeCount.incrementAndGet();
            if (!initCalled) {
                throw new IllegalStateException("Complete called without calling init()");
            }
            if (initCount.get() != NODE_COUNT) {
                throw new IllegalStateException("Complete called without init being called on all the nodes");
            }
        }
    }

    private static final class StuckProcessor implements Processor {
        static CountDownLatch executionStarted;
        static CountDownLatch proceedLatch;

        @Override
        public boolean complete() {
            executionStarted.countDown();
            try {
                proceedLatch.await();
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
            return false;
        }
    }
}
