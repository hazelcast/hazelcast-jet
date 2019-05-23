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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.util.UuidUtil;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JetTestSupport.assertJobStatusEventually;
import static com.hazelcast.jet.core.JetTestSupport.assertTrueEventually;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class SourceBuilder_TopologyChangeTest extends JetTestSupport {

    private static boolean jobRestarted;

    @Test
    public void testRestartJobWhenNodeWasRemoved() {
        testTopologyChange(() -> createJetMember(), node -> node.shutdown());
    }

    @Test
    public void testRestartJobWhenNodeWasAdded() {
        testTopologyChange(() -> null, ignore -> createJetMember());
    }

    private void testTopologyChange(Supplier<JetInstance> initializeSecondMember, Consumer<JetInstance> changeTopology) {
        jobRestarted = false;
        StreamSource<Integer> source = SourceBuilder
                .timestampedStream("src", ctx -> new NumberGeneratorContext())
                .<Integer>fillBufferFn((src, buffer) -> {
                    long expectedCount = NANOSECONDS.toMillis(System.nanoTime() - src.startTime);
                    expectedCount = Math.min(expectedCount, src.current + 100);
                    while (src.current < expectedCount) {
                        buffer.add(src.current, src.current);
                        src.current++;
                    }
                })
                .createSnapshotFn(src -> {
                    System.out.println("Will save " + src.current + " to snapshot");
                    return src;
                })
                .restoreSnapshotFn((src, states) -> {
                    jobRestarted = true;
                    assert states.size() == 1;
                    src.restore(states.get(0));
                    System.out.println("Restored " + src.current + " from snapshot");
                })
                .build();

        JetInstance jet = createJetMember();
        JetInstance possibleSecondNode = initializeSecondMember.get();

        long windowSize = 100;
        IList<WindowResult<Long>> result = jet.getList("result-" + UuidUtil.newUnsecureUuidString());

        Pipeline p = Pipeline.create();
        p.drawFrom(source)
                .withNativeTimestamps(0)
                .window(tumbling(windowSize))
                .aggregate(AggregateOperations.counting())
                .peek()
                .drainTo(Sinks.list(result));

        Job job = jet.newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertTrueEventually(() -> assertFalse("result list is still empty", result.isEmpty()));
        assertJobStatusEventually(job, JobStatus.RUNNING);

        assertFalse(jobRestarted);
        changeTopology.accept(possibleSecondNode);
        assertTrueEventually(() -> assertTrue("restoreSnapshotFn was not called", jobRestarted));

        // wait until more results are added
        int oldSize = result.size();
        assertTrueEventually(() -> assertTrue("no more results added to the list", result.size() > oldSize));

        job.cancel();
        try {
            job.join();
        } catch (CancellationException ignored) {
        }

        // results should contain a monotonic sequence of results, each with count=windowSize
        Iterator<WindowResult<Long>> iterator = result.iterator();
        for (int i = 0; i < result.size(); i++) {
            WindowResult<Long> next = iterator.next();
            assertEquals(windowSize, (long) next.result());
            assertEquals(i * windowSize, next.start());
        }
    }

    private static final class NumberGeneratorContext implements Serializable {

        long startTime = System.nanoTime();
        int current;

        void restore(NumberGeneratorContext other) {
            this.startTime = other.startTime;
            this.current = other.current;
        }
    }
}
