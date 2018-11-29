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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.DummyStatefulP;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataKey;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.JobRepository.SNAPSHOT_DATA_MAP_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class ExportSnapshotTest extends JetTestSupport {

    @Before
    public void before() {
        BlockingMapStore.shouldBlock = true;
        BlockingMapStore.wasBlocked = false;
        TestProcessors.reset(1);
    }

    @Test
    public void when_regularSnapshotInProgress_then_exportWaits() {
        JetConfig config = new JetConfig();
        configureBlockingMapStore(config, SNAPSHOT_DATA_MAP_PREFIX + "*");
        JetInstance instance = createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("v", () -> new StuckProcessor());

        Job job = instance.newJob(dag, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(1));
        assertTrueEventually(() -> assertTrue(BlockingMapStore.wasBlocked));

        Future exportFuture = spawnSafe(() -> job.exportSnapshot("blockedState"));
        assertTrueAllTheTime(() -> assertFalse(exportFuture.isDone()), 2);

        // now release the blocking store, both snapshots should complete
        BlockingMapStore.shouldBlock = false;
        assertTrueEventually(() -> assertTrue(exportFuture.isDone()));
    }

    @Test
    public void when_otherExportInProgress_then_waits() {
        JetConfig config = new JetConfig();
        configureBlockingMapStore(config, JobRepository.EXPORTED_SNAPSHOTS_PREFIX + "*");
        JetInstance instance = createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("v", () -> new StuckProcessor());

        Job job = instance.newJob(dag, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(1));
        assertJobStatusEventually(job, RUNNING);
        JobRepository jr = new JobRepository(instance);
        assertTrueEventually(() -> assertTrue(jr.getJobExecutionRecord(job.getId()).snapshotId() > 1));
        Future exportFuture = spawnSafe(() -> job.exportSnapshot("state"));
        assertTrueEventually(() -> assertTrue(BlockingMapStore.wasBlocked));
        Future exportFuture2 = spawnSafe(() -> job.exportSnapshot("state2"));
        assertTrueAllTheTime(() -> {
            assertFalse(exportFuture.isDone());
            assertFalse(exportFuture2.isDone());
        }, 2);

        // now release the blocking store, both snapshots should complete
        BlockingMapStore.shouldBlock = false;
        assertTrueEventually(() -> assertTrue(exportFuture.isDone() && exportFuture2.isDone()));
        assertFalse(getSnapshotMap(instance, "state").isEmpty());
        assertFalse(getSnapshotMap(instance, "state2").isEmpty());
    }

    @Test
    public void when_snapshottingDisabled_then_exportAndRestoreWorks() {
        JetInstance instance = createJetMember();
        TestProcessors.reset(1);
        DAG dag = new DAG();
        dag.newVertex("v", () -> new DummyStatefulP()).localParallelism(1);
        // When
        Job job = instance.newJob(dag, new JobConfig().setProcessingGuarantee(NONE));
        assertJobStatusEventually(job, RUNNING);
        job.exportSnapshot("exportState");
        // Then1
        assertFalse("exportState is empty", getSnapshotMap(instance, "exportState").isEmpty());
        job.cancelAndExportSnapshot("cancelAndExportState");
        // Then2
        assertFalse("cancelAndExportState is empty",
                getSnapshotMap(instance, "cancelAndExportState").isEmpty());
        assertJobStatusEventually(job, COMPLETED);

        DummyStatefulP.wasRestored = false;
        Job job2 = instance.newJob(dag,
                new JobConfig()
                        .setInitialSnapshotName("cancelAndExportState")
                        .setProcessingGuarantee(NONE));
        assertJobStatusEventually(job2, RUNNING);
        // Then3
        assertTrueEventually(() -> assertTrue(DummyStatefulP.wasRestored));
    }

    @Test
    public void when_targetMapNotEmpty_then_cleared() {
        JetInstance instance = createJetMember();
        IMap<Object, Object> stateMap = getSnapshotMap(instance, "state");
        // When
        stateMap.put("fooKey", "bar");
        DAG dag = new DAG();
        dag.newVertex("v", () -> new StuckProcessor());
        Job job = instance.newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        job.exportSnapshot("state");
        // Then
        assertNull("map was not cleared", stateMap.get("fooKey"));
        assertEquals(1, stateMap.size());
    }

    @Test
    public void when_nonExistentSnapshot() {
        JetInstance instance = createJetMember();
        assertNull("snapshot should be null" , instance.getJobStateSnapshot("state"));
    }

    @Test
    public void test_exportStateWhileSuspended() {
        test_exportStateWhileSuspended(false);
    }

    @Test
    public void test_exportStateAndCancelWhileSuspended() {
        test_exportStateWhileSuspended(true);
    }

    @Test
    public void when_initialSnapshotSetAndJobFailsBeforeCreatingAnotherSnapshot_then_initialSnapshotUsedAgain() {
        TestProcessors.reset(2);
        DAG dag = new DAG();
        dag.newVertex("p", DummyStatefulP::new).localParallelism(1);
        JetInstance[] instances = createJetMembers(new JetConfig(), 2);
        Job job = instances[0].newJob(dag,
                new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(10));
        // wait for the first snapshot
        JobRepository jr = new JobRepository(instances[0]);
        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() ->
                assertTrue("no first snapshot", jr.getJobExecutionRecord(job.getId()).snapshotId() >= 0));
        job.cancelAndExportSnapshot("state");
        DummyStatefulP.wasRestored = false;

        // When
        Job job2 = instances[0].newJob(dag, new JobConfig().setProcessingGuarantee(NONE).setInitialSnapshotName("state"));
        assertTrueEventually(() -> assertTrue(DummyStatefulP.wasRestored));
        DummyStatefulP.wasRestored = false;
        instances[1].getHazelcastInstance().getLifecycleService().terminate();

        // Then
        assertTrueEventually(() -> assertTrue(DummyStatefulP.wasRestored));
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job2.getStatus()), 1);
    }

    @Test
    public void when_snapshotValidationFails_then_snapshotNotUsed() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new StuckProcessor());
        JetInstance instance = createJetMember();
        Job job = instance.newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        JobStateSnapshot state = job.cancelAndExportSnapshot("state");

        // When - cause the snapshot to be invalid
        getSnapshotMap(instance, state.name()).put("foo", "bar");

        job = instance.newJob(dag, new JobConfig().setInitialSnapshotName("state"));
        assertJobStatusEventually(job, FAILED);
    }

    @Test
    public void when_entryWithDifferentSnapshotIdFound_then_fallbackValidationUsed() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new StuckProcessor());
        JetInstance instance = createJetMember();
        Job job = instance.newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        JobStateSnapshot state = job.cancelAndExportSnapshot("state");

        // When - cause the snapshot to be partly invalid - insert entry with wrong snapshot ID
        getSnapshotMap(instance, state.name()).put(new SnapshotDataKey(1, -10, "vertex", 1), "bar");

        Job job2 = instance.newJob(dag, new JobConfig().setInitialSnapshotName("state"));
        assertJobStatusEventually(job2, RUNNING);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job2.getStatus()), 1);
    }

    private void test_exportStateWhileSuspended(boolean cancel) {
        JetInstance instance = createJetMember();
        DAG dag = new DAG();
        dag.newVertex("v", () -> new StuckProcessor());
        Job job = instance.newJob(dag, new JobConfig().setSnapshotIntervalMillis(10).setProcessingGuarantee(EXACTLY_ONCE));
        JobRepository jr = new JobRepository(instance);
        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertTrue(jr.getJobExecutionRecord(job.getId()).snapshotId() >= 0));
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        if (cancel) {
            job.cancelAndExportSnapshot("state");
        } else {
            job.exportSnapshot("state");
        }
        assertFalse("state map is empty", getSnapshotMap(instance, "state").isEmpty());
        if (cancel) {
            assertJobStatusEventually(job, COMPLETED);
        } else {
            assertTrueAllTheTime(() -> assertEquals(SUSPENDED, job.getStatus()), 1);
            job.resume();
            assertJobStatusEventually(job, RUNNING);
        }
    }

    public static IMapJet<Object, Object> getSnapshotMap(JetInstance instance, String snapshotName) {
        return instance.getMap(JobRepository.exportedSnapshotMapName(snapshotName));
    }

    private void configureBlockingMapStore(JetConfig config, String mapName) {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.getMapStoreConfig()
                 .setEnabled(true)
                 .setClassName(BlockingMapStore.class.getName());
        config.getHazelcastConfig().addMapConfig(mapConfig);
    }

    /**
     * A MapStore that will block map operations until unblocked.
     */
    private static class BlockingMapStore implements MapStore {
        private static volatile boolean shouldBlock;
        private static volatile boolean wasBlocked;

        @Override
        public void store(Object key, Object value) {
            block();
        }

        @Override
        public void storeAll(Map map) {
            block();
        }

        @Override
        public void delete(Object key) {
            block();
        }

        @Override
        public void deleteAll(Collection keys) {
            block();
        }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            return null;
        }

        @Override
        public Iterable loadAllKeys() {
            return null;
        }

        private void block() {
            while (shouldBlock) {
                wasBlocked = true;
                sleepMillis(100);
            }
        }
    }
}
