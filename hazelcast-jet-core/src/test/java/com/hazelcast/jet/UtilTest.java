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

package com.hazelcast.jet;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl.SnapshotDataKey;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
public class UtilTest extends JetTestSupport {

    @Test
    public void when_entry() {
        Entry<String, Integer> e = entry("key", 1);
        assertEquals("key", e.getKey());
        assertEquals(Integer.valueOf(1), e.getValue());
    }

    @Test
    public void test_idToString() {
        assertEquals("0000-0000-0000-0000", idToString(0));
        assertEquals("0000-0000-0000-0001", idToString(1));
        assertEquals("7fff-ffff-ffff-ffff", idToString(Long.MAX_VALUE));
        assertEquals("8000-0000-0000-0000", idToString(Long.MIN_VALUE));
        assertEquals("ffff-ffff-ffff-ffff", idToString(-1));
        assertEquals("1122-10f4-7de9-8115", idToString(1234567890123456789L));
        assertEquals("eedd-ef0b-8216-7eeb", idToString(-1234567890123456789L));
    }

    @Test
    public void test_cleanUpSnapshotMap() {
        DAG dag = new DAG();
        TestProcessors.reset(1);
        dag.newVertex("v", () -> new StuckProcessor());

        JetInstance instance = createJetMember();
        Job job = instance.newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        job.cancelAndExportState("state");
        IMapJet<Object, Object> state = instance.getExportedState("state");
        // insert a mess to the exported state map
        state.put(new SnapshotDataKey(1, 9999, "vertex", 0), "mess");

        System.out.println("here");
        job = instance.newJob(dag, new JobConfig().setInitialSnapshotName("state"));
        try {
            job.join();
            fail("join() should have failed");
        } catch (Exception e) {
            assertContains(e.getCause().getMessage(), "probably corrupted");
        }

        Util.cleanUpSnapshotMap(instance, "state");
        job = instance.newJob(dag, new JobConfig().setInitialSnapshotName("state"));
        StuckProcessor.proceedLatch.countDown();
        job.join();
    }
}
