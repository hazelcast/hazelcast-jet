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
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.function.SupplierEx;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for JobMetrics that don't use shared cluster.
 */
public class JobMetrics_NonSharedClusterTest extends JetTestSupport {

    @Before
    public void before() {
        TestProcessors.reset(1);
    }

    @Test
    public void when_metricsCollectionOff_then_emptyMetrics() {
        JetConfig config = new JetConfig();
        config.getMetricsConfig().setEnabled(false);
        JetInstance inst = createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("v1", (SupplierEx<Processor>) NoOutputSourceP::new).localParallelism(1);
        Job job = inst.newJob(dag);
        assertEquals(0, job.getMetrics().size());
    }

    @Test
    public void when_metricsIntervalHuge_then_emptyMetrics() {
        JetConfig config = new JetConfig();
        config.getMetricsConfig().setCollectionIntervalSeconds(10_000);
        JetInstance inst = createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("v1", (SupplierEx<Processor>) NoOutputSourceP::new).localParallelism(1);
        // todo make this test deterministic: the initial collection delay is 1s. We sleep 2s, that should work,
        //  but for reliability it should be at least 10 seconds, which will slow down the test. We can submit one
        //  job, wait until it has metrics, then submit another and check that it doesn't. This test is designed
        //  to ensure that the job.getMetrics() call doesn't block until collection takes place
        sleepSeconds(2);
        Job job = inst.newJob(dag);
        JobMetrics metrics = job.getMetrics();
        assertEquals(metrics.toString(), 0, metrics.size());
    }
}
