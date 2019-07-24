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
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestUtil.assertExceptionInCauses;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class JobMetricsTest extends TestInClusterSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        TestProcessors.reset(MEMBER_COUNT * parallelism);
    }

    @Test
    public void test_retrieveJobMetrics_member() throws Throwable {
        test_retrieveJobMetrics(member);
    }

    @Test
    public void test_retrieveJobMetrics_client() throws Throwable {
        test_retrieveJobMetrics(client);
    }

    private void test_retrieveJobMetrics(JetInstance jetInstance) throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("v1", TestProcessors.MockP::new);
        dag.newVertex("v2", (SupplierEx<Processor>) TestProcessors.NoOutputSourceP::new);
        Job job = jetInstance.newJob(dag);

        TestProcessors.NoOutputSourceP.executionStarted.await();
        assertEquals(JobStatus.RUNNING, job.getStatus());

        JetTestSupport.assertTrueEventually(() -> assertJobHasMetrics(job));

        TestProcessors.NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertEquals(JobStatus.COMPLETED, job.getStatus());
        assertJobHasMetrics(job);
    }

    @Test
    public void when_jobFailedBeforeStarted_then_emptyMetrics() {
        DAG dag = new DAG();
        RuntimeException exc = new RuntimeException("foo");
        // Job will fail in ProcessorSupplier.init method, which is called before InitExecutionOp is
        // sent. That is before any member ever knew of the job.
        dag.newVertex("v1", new MockPS(MockP::new, 1).setInitError(exc));

        Job job = member.newJob(dag);
        try {
            job.join();
            fail("job didn't fail");
        } catch (Exception e) {
            assertExceptionInCauses(exc, e);
        }

        assertEquals(0, job.getMetrics().size());
    }

    @Test
    public void when_jobNotYetRunning_then_emptyMetrics() {
        DAG dag = new DAG();
        BlockingInInitMetaSupplier.latch = new CountDownLatch(1);
        dag.newVertex("v1", new BlockingInInitMetaSupplier());

        Job job = member.newJob(dag);
        assertTrueAllTheTime(() -> assertEquals(0, job.getMetrics().size()), 2);
        BlockingInInitMetaSupplier.latch.countDown();
        assertTrueEventually(() -> {
            try {
                assertTrue(job.getMetrics().size() > 0);
            } catch (Exception e) {
                System.out.println("boo, e=" + e);
                throw e;
            }
        });
    }

    @Test
    public void when_jobSuspended_then_lastExecutionMetricsReturned() throws Throwable {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", TestProcessors.MockP::new);
        Vertex v2 = dag.newVertex("v2", (SupplierEx<Processor>) TestProcessors.NoOutputSourceP::new);
        dag.edge(between(v1, v2));

        Job job = member.newJob(dag);
        TestProcessors.NoOutputSourceP.executionStarted.await();
        assertEquals(JobStatus.RUNNING, job.getStatus());
        JetTestSupport.assertTrueEventually(() -> assertJobHasMetrics(job));

        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        JetTestSupport.assertTrueEventually(() -> assertJobHasMetrics(job));

        job.resume();
        assertJobStatusEventually(job, RUNNING);
        JetTestSupport.assertTrueEventually(() -> assertJobHasMetrics(job));

        TestProcessors.NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertEquals(JobStatus.COMPLETED, job.getStatus());
        assertJobHasMetrics(job);
    }

    @Test
    public void test_jobRestarted() throws Throwable {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", TestProcessors.MockP::new);
        Vertex v2 = dag.newVertex("v2", (SupplierEx<Processor>) TestProcessors.NoOutputSourceP::new);
        dag.edge(between(v1, v2));

        Job job = member.newJob(dag);
        TestProcessors.NoOutputSourceP.executionStarted.await();
        assertEquals(JobStatus.RUNNING, job.getStatus());

        job.restart();
        JetTestSupport.assertEqualsEventually(job::getStatus, JobStatus.RUNNING);
        JetTestSupport.assertTrueEventually(() -> assertJobHasMetrics(job));

        TestProcessors.NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertEquals(JobStatus.COMPLETED, job.getStatus());
        assertJobHasMetrics(job);
    }

    @Test
    public void test_jobFailed() {
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex source = dag.newVertex("source", TestProcessors.ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex(
                "faulty",
                new TestProcessors.MockPMS(() ->
                        new TestProcessors.MockPS(() -> new TestProcessors.MockP().setProcessError(e), MEMBER_COUNT)));
        dag.edge(between(source, process));

        Job job = runJobExpectFailure(dag, e);
        assertEquals(JobStatus.FAILED, job.getStatus());
        assertJobHasMetrics(job);
    }

    private Job runJobExpectFailure(@Nonnull DAG dag, @Nonnull RuntimeException expectedException) {
        Job job = null;
        try {
            job = member.newJob(dag);
            job.join();
            fail("Job execution should have failed");
        } catch (Exception actual) {
            Throwable cause = peel(actual);
            assertContains(cause.getMessage(), expectedException.getMessage());
        }
        return job;
    }

    private void assertJobHasMetrics(Job job) {
        assertTrue(job.getMetrics().size() > 0);
        assertTrue(job.getMetrics().withTag(MetricTags.METRIC, "queuesSize").size() > 0);
    }

    private static class BlockingInInitMetaSupplier implements ProcessorMetaSupplier {
        static CountDownLatch latch;

        @Override
        public void init(@Nonnull Context context) throws Exception {
            latch.await();
        }

        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return a -> new MockPS(NoOutputSourceP::new, 1);
        }
    }
}
