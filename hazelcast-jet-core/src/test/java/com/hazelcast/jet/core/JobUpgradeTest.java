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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_GRACEFUL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class JobUpgradeTest extends JetTestSupport {

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
    public void when_cancelWithSnapshotAndResubmit_then_jobContinues() throws Exception {
        // When
        // snapshot export should work also with snapshotting disabled
        Job job = instances[0].newJob(dag, new JobConfig().setProcessingGuarantee(NONE));
        StuckProcessor.executionStarted.await();
        job.cancelAndExportState("state");
        assertJobStatusEventually(job, FAILED);
        try {
            job.getFuture().get();
            fail("ExecutionException was expected");
        } catch (ExecutionException expected) {
            assertInstanceOf(JobTerminateRequestedException.class, expected.getCause());
            assertEquals(CANCEL_GRACEFUL, ((JobTerminateRequestedException) expected.getCause()).mode());
        }
        Job job2 = instances[0].newJob(dag, new JobConfig().setInitialSnapshotName("state"));
        assertJobStatusEventually(job2, RUNNING);
        assertEquals(FAILED, job.getStatus());
        StuckProcessor.proceedLatch.countDown();
        job2.join();

        // Then
        assertEquals(2 * NODE_COUNT, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertNull(MockPS.receivedCloseErrors.stream()
                                                 .filter(e -> !(e instanceof JobTerminateRequestedException))
                                                 .findFirst()
                                                 .orElse(null));
        }, 5);
    }
}
