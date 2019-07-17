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
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class JobMetricsTest extends TestInClusterSupport {

    @Before
    public void setup() {
        TestProcessors.reset(MEMBER_COUNT * parallelism);
    }

    @Test
    public void memberCanRetrieveJobMetrics() throws Throwable {
        instanceCanRetrieveJobMetrics(member);
    }

    @Test
    public void clientCanRetriveJobMetrics() throws Throwable {
        instanceCanRetrieveJobMetrics(client);
    }

    private void instanceCanRetrieveJobMetrics(JetInstance jetInstance) throws Throwable {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", TestProcessors.MockP::new);
        Vertex v2 = dag.newVertex("v2", (SupplierEx<Processor>) TestProcessors.NoOutputSourceP::new);
        dag.edge(between(v1, v2));

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
    public void metricsForRestartedJobs() throws Throwable {
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
    public void metricsForFailedJob() {
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

}
