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
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.Edge.between;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    private void instanceCanRetrieveJobMetrics(JetInstance jetInstance) throws InterruptedException {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", TestProcessors.MockP::new);
        Vertex v2 = dag.newVertex("v2", () -> new TestProcessors.NoOutputSourceP());
        dag.edge(between(v1, v2));

        Job job = jetInstance.newJob(dag);

        TestProcessors.NoOutputSourceP.executionStarted.await();
        TestProcessors.NoOutputSourceP.proceedLatch.countDown();
        assertEquals(JobStatus.RUNNING, job.getStatus());

        JetTestSupport.assertTrueEventually(
                () -> assertTrue(job.getMetrics().size() > 0));
        JetTestSupport.assertTrueEventually(
                () -> assertTrue(job.getMetrics().withTag("metric", "queuesSize").size() > 0));

        job.join();
        assertEquals(JobStatus.COMPLETED, job.getStatus());
        assertTrue(job.getMetrics().size() > 0);
        assertTrue(job.getMetrics().withTag("metric", "queuesSize").size() > 0);
    }

}
