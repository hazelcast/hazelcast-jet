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

package com.hazelcast.jet.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.operation.InitOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.SHUTDOWN_REQUEST;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook.EXECUTE_OP;
import static com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook.INIT_OP;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class TopologyChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;

    private static final int PARALLELISM = 4;

    private static final int TIMEOUT_MILLIS = 8000;

    @Parameterized.Parameter
    public boolean[] liteMemberFlags;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private int nodeCount;

    private JetInstance[] instances;
    private JetTestInstanceFactory factory;
    private JetConfig config;

    @Parameterized.Parameters
    public static Collection<boolean[]> parameters() {
        return Arrays.asList(new boolean[][] {
                {false, false, false},
                {true, false, false},
                {false, true, false}
        });
    }

    @Before
    public void setup() {
        nodeCount = 0;
        for (boolean isLiteMember : liteMemberFlags) {
            if (!isLiteMember) {
                nodeCount++;
            }
        }

        MockPS.completeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(nodeCount * PARALLELISM);

        factory = new JetTestInstanceFactory();
        config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
        config.getHazelcastConfig().getProperties().put(OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));

        instances = new JetInstance[NODE_COUNT];

        for (int i = 0; i < NODE_COUNT; i++) {
            JetConfig config = new JetConfig();
            config.getHazelcastConfig().setLiteMember(liteMemberFlags[i]);
            config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
            config.getHazelcastConfig().getProperties().put(OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                    Integer.toString(TIMEOUT_MILLIS));

            instances[i] = factory.newMember(config);
        }
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void when_addNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        factory.newMember();
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(nodeCount, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(nodeCount, MockPS.completeCount.get());
            assertThat(MockPS.completeErrors, empty());
        });
    }

    @Test
    public void when_addAndRemoveNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        JetInstance instance = factory.newMember();
        instance.shutdown();
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(nodeCount, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(nodeCount, MockPS.completeCount.get());
            assertThat(MockPS.completeErrors, empty());
        });
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecution_then_jobRestarts() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        job.join();

        // upon non-coordinator member leave, remaining members restart and complete the job
        final int count = nodeCount * 2 - 1;
        assertEquals(count, MockPS.initCount.get());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(count, MockPS.completeCount.get());
                assertEquals(nodeCount, MockPS.completeErrors.size());
                for (int i = 0; i < MockPS.completeErrors.size(); i++) {
                    Throwable error = MockPS.completeErrors.get(i);
                    assertTrue(error instanceof TopologyChangedException
                            || error instanceof HazelcastInstanceNotActiveException);
                }
            }
        });
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecutionAndNoRestartConfigured_then_jobFails() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));
        JobConfig config = new JobConfig().setAutoRestartOnMemberFailure(false);

        // When
        Job job = instances[0].newJob(dag, config);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        Throwable ex = job.getFuture().handle((r, e) -> e).get();
        assertInstanceOf(TopologyChangedException.class, ex);
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecution_then_clientStillGetsJobResult() throws Throwable {
        // Given
        JetClientInstanceImpl client = factory.newClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = client.newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        job.join();
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_jobCompletes() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Long jobId = null;
        try {
            Job job = instances[0].newJob(dag);
            Future<Void> future = job.getFuture();
            jobId = job.getJobId();
            StuckProcessor.executionStarted.await();

            instances[0].getHazelcastInstance().getLifecycleService().terminate();
            StuckProcessor.proceedLatch.countDown();

            future.get();
            fail();
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof HazelcastInstanceNotActiveException);
        }

        // Then
        assertNotNull(jobId);
        final long completedJobId = jobId;

        JobCoordinationService coordService = getJetService(instances[1]).getJobCoordinationService();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                JobResult jobResult = coordService.getJobResult(completedJobId);
                assertNotNull(jobResult);
                assertTrue(jobResult.isSuccessful());
            }
        });

        final int count = liteMemberFlags[0] ? (2 * nodeCount) : (2 * nodeCount - 1);
        assertEquals(count, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(count, MockPS.completeCount.get());
            assertEquals(nodeCount, MockPS.completeErrors.size());
            for (int i = 0; i < MockPS.completeErrors.size(); i++) {
                Throwable error = MockPS.completeErrors.get(i);
                assertTrue(error instanceof TopologyChangedException
                        || error instanceof HazelcastInstanceNotActiveException);
            }
        });
    }

    @Test
    public void when_coordinatorLeavesDuringExecutionAndNoRestartConfigured_then_jobFails() throws Throwable {
        // Given
        JetClientInstanceImpl client = factory.newClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));
        JobConfig config = new JobConfig().setAutoRestartOnMemberFailure(false);

        // When
        Job job = client.newJob(dag, config);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        Throwable ex = job.getFuture().handle((r, e) -> e).get();
        assertInstanceOf(TopologyChangedException.class, ex);
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_nonCoordinatorJobSubmitterStillGetsJobResult()
            throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[1].newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[0].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        // Then
        job.join();
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_clientStillGetsJobResult() throws Throwable {
        // Given
        JetClientInstanceImpl client = factory.newClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = client.newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[0].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        // Then
        job.join();
    }

    @Test
    public void when_jobParticipantHasStaleMemberList_then_jobInitRetries() throws Throwable {
        // Given
        dropOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                ClusterDataSerializerHook.F_ID, singletonList(MEMBER_INFO_UPDATE));
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount + 1)));


        // When
        factory.newMember(config);
        Job job = instances[0].newJob(dag);


        // Then
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(STARTING, job.getJobStatus());
            }
        }, 5);

        resetPacketFiltersFrom(instances[0].getHazelcastInstance());

        job.join();
    }

    @Test
    public void when_jobParticipantReceivesStaleInitOperation_then_jobRestarts() throws Throwable {
        // Given
        JetInstance newInstance = factory.newMember(config);
        for (JetInstance instance : instances) {
            assertClusterSizeEventually(NODE_COUNT + 1, instance.getHazelcastInstance());
        }

        dropOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(INIT_OP));

        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount + 1)));

        Job job = instances[0].newJob(dag);
        JetService jetService = getJetService(instances[0]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(jetService.getJobCoordinationService().getMasterContexts().isEmpty());
            }
        });

        MasterContext masterContext = jetService.getJobCoordinationService().getMasterContext(job.getJobId());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(STARTING, masterContext.jobStatus());
                assertNotEquals(0, masterContext.getExecutionId());
            }
        });


        // When
        long executionId = masterContext.getExecutionId();
        newInstance.getHazelcastInstance().getLifecycleService().terminate();
        for (JetInstance instance : instances) {
            assertClusterSizeEventually(NODE_COUNT, instance.getHazelcastInstance());
        }

        resetPacketFiltersFrom(instances[0].getHazelcastInstance());


        // Then
        job.join();
        assertNotEquals(executionId, masterContext.getExecutionId());
    }

    @Test
    public void when_nodeIsShuttingDownDuringInit_then_jobRestarts() throws Throwable {
        // Given that newInstance will have a long shutdown process
        for (JetInstance instance : instances) {
            warmUpPartitions(instance.getHazelcastInstance());
        }

        dropOperationsBetween(instances[2].getHazelcastInstance(), instances[0].getHazelcastInstance(),
                PartitionDataSerializerHook.F_ID, singletonList(SHUTDOWN_REQUEST));
        dropOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(INIT_OP));

        // When a job participant starts its shutdown after the job is submitted
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount - 1)));

        Job job = instances[0].newJob(dag);

        JetService jetService = getJetService(instances[0]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(jetService.getJobCoordinationService().getMasterContexts().isEmpty());
            }
        });

        spawn(instances[2]::shutdown);

        // Then, it restarts until the shutting down node is gone
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(STARTING, job.getJobStatus());
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(STARTING, job.getJobStatus());
            }
        }, 5);

        resetPacketFiltersFrom(instances[2].getHazelcastInstance());

        job.join();
    }

    @Test
    public void when_nodeIsShuttingDownAfterInit_then_jobRestarts() throws Throwable {
        // Given that the second node has not received ExecuteOperation yet
        for (JetInstance instance : instances) {
            warmUpPartitions(instance.getHazelcastInstance());
        }

        dropOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(EXECUTE_OP));

        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount - 1)));

        Job job = instances[0].newJob(dag);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(RUNNING, job.getJobStatus());
            }
        });

        // When a participant shuts down during execution
        instances[2].shutdown();


        // Then, the job restarts and successfully completes
        job.join();
    }

    @Test
    public void when_nodeIsNotJobParticipant_then_initFails() throws Throwable {
        int jobId = 1;
        int executionId = 1;
        HazelcastInstance master = instances[0].getHazelcastInstance();
        int memberListVersion = getClusterService(master).getMemberListVersion();
        Set<MemberInfo> memberInfos = new HashSet<>();
        for (int i = 1; i < instances.length; i++) {
            memberInfos.add(new MemberInfo(getNode(instances[i].getHazelcastInstance()).getLocalMember()));
        }
        ExecutionPlan plan = mock(ExecutionPlan.class);

        InitOperation op = new InitOperation(jobId, executionId, memberListVersion, memberInfos, plan);
        Future<Object> future = getOperationService(master)
                .createInvocationBuilder(JetService.SERVICE_NAME, op, getAddress(master))
                .invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }
}
