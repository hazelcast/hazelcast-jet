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

import com.hazelcast.jet.TopologyChangeTest.MockSupplier;
import com.hazelcast.jet.TopologyChangeTest.StuckProcessor;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.JobStatus.COMPLETED;
import static com.hazelcast.jet.JobStatus.RESTARTING;
import static com.hazelcast.jet.JobStatus.STARTING;
import static com.hazelcast.jet.TestUtil.getJetService;
import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class SplitBrainTest extends JetSplitBrainTestSupport2 {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Override
    protected void onBeforeSetup() {
        MockSupplier.completeCount.set(0);
        MockSupplier.initCount.set(0);
        MockSupplier.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
    }

    @Override
    protected void onJetConfigCreated(JetConfig jetConfig) {
        jetConfig.setJobMetadataBackupCount(MAX_BACKUP_COUNT);
    }

    @Test
    public void when_quorumIsLostOnMinority_then_jobRestartsUntilMerge() {
        int firstSubClusterSize = 3;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<JetInstance[]> beforeSplit = instances -> {
            MockSupplier processorSupplier = new MockSupplier(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].newJob(dag);
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            StuckProcessor.proceedLatch.countDown();

            assertTrueEventually(() ->
                    assertEquals(clusterSize + firstSubClusterSize, MockSupplier.initCount.get()));

            long jobId = jobRef[0].getJobId();

            assertTrueEventually(() -> {
                JetService service = getJetService(firstSubCluster[0]);
                assertEquals(COMPLETED, service.getJobStatus(jobId));
            });

            assertTrueEventually(() -> {
                JetService service = getJetService(secondSubCluster[0]);
                assertEquals(STARTING, service.getJobStatus(jobId));
            });

            assertTrueAllTheTime(() -> {
                JetService service = getJetService(secondSubCluster[0]);
                assertEquals(STARTING, service.getJobStatus(jobId));
            }, 20);
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize + firstSubClusterSize, MockSupplier.initCount.get());
                assertEquals(clusterSize + firstSubClusterSize, MockSupplier.completeCount.get());
            });

            assertEquals(clusterSize, MockSupplier.completeErrors.size());
            MockSupplier.completeErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_quorumIsLostOnBothSides_then_jobRestartsUntilMerge() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<JetInstance[]> beforeSplit = instances -> {
            MockSupplier processorSupplier = new MockSupplier(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].newJob(dag);
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            StuckProcessor.proceedLatch.countDown();

            long jobId = jobRef[0].getJobId();

            assertTrueEventually(() -> {
                JetService service1 = getJetService(firstSubCluster[0]);
                JetService service2 = getJetService(secondSubCluster[0]);
                assertEquals(RESTARTING, service1.getJobStatus(jobId));
                assertEquals(STARTING, service2.getJobStatus(jobId));
            });

            assertTrueAllTheTime(() -> {
                JetService service1 = getJetService(firstSubCluster[0]);
                JetService service2 = getJetService(secondSubCluster[0]);
                assertEquals(RESTARTING, service1.getJobStatus(jobId));
                assertEquals(STARTING, service2.getJobStatus(jobId));
            }, 20);
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize * 2, MockSupplier.initCount.get());
                assertEquals(clusterSize * 2, MockSupplier.completeCount.get());
            });

            assertEquals(clusterSize, MockSupplier.completeErrors.size());
            MockSupplier.completeErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_splitBrainProtectionIsDisabled_then_jobCompletesOnBothSides() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 2;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(clusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        Consumer<JetInstance[]> beforeSplit = instances -> {
            MockSupplier processorSupplier = new MockSupplier(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = instances[0].newJob(dag, new JobConfig().setSplitBrainProtectionEnabled(false));
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            StuckProcessor.proceedLatch.countDown();

            long jobId = jobRef[0].getJobId();

            assertTrueEventually(() -> {
                JetService service1 = getJetService(firstSubCluster[0]);
                JetService service2 = getJetService(secondSubCluster[0]);
                assertEquals(COMPLETED, service1.getJobStatus(jobId));
                assertEquals(COMPLETED, service2.getJobStatus(jobId));
            });
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            assertTrueEventually(() -> {
                assertEquals(clusterSize * 2, MockSupplier.initCount.get());
                assertEquals(clusterSize * 2, MockSupplier.completeCount.get());
            });

            assertEquals(clusterSize, MockSupplier.completeErrors.size());
            MockSupplier.completeErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge);
    }

    @Test
    public void when_jobIsSubmittedToMinoritySide_then_jobCompletesAfterMerge() {
        int firstSubClusterSize = 2;
        int secondSubClusterSize = 1;
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        StuckProcessor.executionStarted = new CountDownLatch(secondSubClusterSize * PARALLELISM);
        Job[] jobRef = new Job[1];

        BiConsumer<JetInstance[], JetInstance[]> onSplit = (firstSubCluster, secondSubCluster) -> {
            MockSupplier processorSupplier = new MockSupplier(StuckProcessor::new, clusterSize);
            DAG dag = new DAG().vertex(new Vertex("test", processorSupplier));
            jobRef[0] = secondSubCluster[0].newJob(dag);
            assertOpenEventually(StuckProcessor.executionStarted);
        };

        Consumer<JetInstance[]> afterMerge = instances -> {
            StuckProcessor.proceedLatch.countDown();

            long jobId = jobRef[0].getJobId();

            assertTrueEventually(() -> {
                try {
                    JetService service = getJetService(instances[0]);
                    assertEquals(COMPLETED, service.getJobStatus(jobId));
                } catch (IllegalStateException ignored) {
                    // job status not found may be received until merge is done
                }
            });

            assertTrueEventually(() -> {
                assertEquals(clusterSize + secondSubClusterSize, MockSupplier.initCount.get());
                assertEquals(clusterSize + secondSubClusterSize, MockSupplier.completeCount.get());
            });

            assertEquals(secondSubClusterSize, MockSupplier.completeErrors.size());
            MockSupplier.completeErrors.forEach(t -> assertTrue(t instanceof TopologyChangedException));
        };

        testSplitBrain(firstSubClusterSize, secondSubClusterSize, null, onSplit, afterMerge);
    }

}
