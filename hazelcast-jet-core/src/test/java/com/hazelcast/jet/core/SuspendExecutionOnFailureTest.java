/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPMS;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SuspendExecutionOnFailureTest extends TestInClusterSupport {

    private static final Throwable MOCK_ERROR = new AssertionError("mock error");

    private JobConfig jobConfig;

    @Before
    public void before() {
        jobConfig = new JobConfig().setSuspendOnFailure(true);
        TestProcessors.reset(0);
    }

    @Test
    public void when_jobRunning_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));
        Job job = jet().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");

        cancelAndJoin(job);
    }

    @Test
    public void when_jobCompleted_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));
        Job job = jet().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);

        // When
        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(job, COMPLETED);

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");

        job.join();
    }

    @Test
    public void when_jobSuspendedByUser_then_suspensionCauseSaysSo() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));

        // When
        Job job = jet().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        assertEquals("Requested by user", job.getSuspensionCause());

        cancelAndJoin(job);
    }

    @Test
    public void when_jobSuspendedDueToFailure_then_suspensionCauseDescribeProblem() {
        // Given
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setProcessError(MOCK_ERROR), MEMBER_COUNT)));
        dag.edge(between(source, process));

        // When
        Job job = jet().newJob(dag, jobConfig);

        // Then
        assertJobStatusEventually(job, JobStatus.SUSPENDED);
        assertTrue(job.getSuspensionCause().startsWith("Due to failure:\n" +
                "com.hazelcast.jet.JetException: Exception in ProcessorTasklet{faulty#0}: " +
                "java.lang.AssertionError: mock error"));

        cancelAndJoin(job);
    }

}
