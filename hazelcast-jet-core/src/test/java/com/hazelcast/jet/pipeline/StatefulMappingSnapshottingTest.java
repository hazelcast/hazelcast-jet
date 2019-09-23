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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatefulMappingSnapshottingTest extends JetTestSupport {

    private static final String SOURCE_MAP_NAME
            = StatefulMappingSnapshottingTest.class.getSimpleName() + "_map";
    private static final String SNAPSHOT_NAME
            = StatefulMappingSnapshottingTest.class.getSimpleName() + "_snapshot";

    private static final int SNAPSHOT_ITERATIONS = 10;
    private static final int ITEMS_IN_ONE_ROUND = 1000;

    private static AtomicLong functionCall;
    private JetInstance instance;
    private Job job;

    @Before
    public void setup() {
        functionCall = new AtomicLong(0);
        job = null;
        JetConfig config = new JetConfig();
        config.getHazelcastConfig()
                .addEventJournalConfig(new EventJournalConfig()
                        .setMapName(SOURCE_MAP_NAME)
                        .setCapacity(10000)
                        .setTimeToLiveSeconds(0));
        instance = createJetMembers(config, 2)[0];
    }

    @Test
    public void mapStateful_whenStartedFromSnapshot_thenStateInGeneralStageIsFromSnapshot() {
        runTestForGeneralStage(globalStage -> globalStage.mapStateful(AtomicLong::new, (state, input) -> {
            functionCall.set(state.incrementAndGet());
            return null;
        }));
    }

    @Test
    public void flatMapStateful_whenStartedFromSnapshot_thenStateInGeneralStageIsFromSnapshot() {
        runTestForGeneralStage(globalStage -> globalStage.flatMapStateful(AtomicLong::new, (state, input) -> {
            functionCall.set(state.incrementAndGet());
            return Traversers.empty();
        }));
    }

    @Test
    public void filterStateful_whenStartedFromSnapshot_thenStateInGeneralStageIsFromSnapshot() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.mapJournal(SOURCE_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .filterStateful(AtomicLong::new, (state, input) -> {
                    functionCall.set(state.incrementAndGet());
                    return false;
                })
                .drainTo(Sinks.logger());
        runTest(p);
    }

    @Test
    public void mapStateful_whenStartedFromSnapshot_thenStateInGeneralStageWithKeyIsFromSnapshot() {
        runTestForGeneralStageWithKey(globalStage
                -> globalStage.mapStateful(AtomicLong::new, (state, key, input) -> {
                    functionCall.set(state.incrementAndGet());
                    return null;
                }));
    }

    @Test
    public void flatMapStateful_whenStartedFromSnapshot_thenStateInGeneralStageWithKeyIsFromSnapshot() {
        runTestForGeneralStageWithKey(globalStage
                -> globalStage.flatMapStateful(AtomicLong::new, (state, key, input) -> {
                    functionCall.set(state.incrementAndGet());
                    return Traversers.empty();
                }));
    }

    @Test
    public void filterStateful_whenStartedFromSnapshot_thenStateInGeneralStageIsWithKeyFromSnapshot() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, Integer>mapJournal(SOURCE_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .groupingKey(t -> t.getKey() % 1)
                .filterStateful(AtomicLong::new, (state, input) -> {
                    functionCall.set(state.incrementAndGet());
                    return false;
                })
                .drainTo(Sinks.logger());
        runTest(p);
    }

    private void runTestForGeneralStage(
            Function<StreamStage<Map.Entry<Object, Object>>, StreamStage<Object>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStage<Map.Entry<Object, Object>> globalStage
                = p.drawFrom(Sources.mapJournal(SOURCE_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT))
                        .withIngestionTimestamps();
        StreamStage<Object> statefulStage = statefulFn.apply(globalStage);
        statefulStage.drainTo(Sinks.logger());
        runTest(p);
    }

    private void runTestForGeneralStageWithKey(
            Function<StreamStageWithKey<Map.Entry<Integer, Integer>, Integer>, StreamStage<Object>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStageWithKey<Map.Entry<Integer, Integer>, Integer> streamStageWithKey
                = p.drawFrom(Sources.<Integer, Integer>mapJournal(
                        SOURCE_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT))
                        .withIngestionTimestamps()
                        .groupingKey(t -> t.getKey() % 1);
        StreamStage<Object> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.drainTo(Sinks.logger());
        runTest(p);
    }

    private void runTest(Pipeline p) {
        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        job = instance.newJob(p, config);
        assertTrueEventually(() -> {
            assertEquals(JobStatus.RUNNING, job.getStatus());
        });

        Map<Integer, Integer> map = instance.getMap(SOURCE_MAP_NAME);
        assertTrue(map.isEmpty());

        for (int i = 0; i < SNAPSHOT_ITERATIONS; i++) {
            final int expectedSizeAfterThisRound = ITEMS_IN_ONE_ROUND * (i + 1);
            for (int j = ITEMS_IN_ONE_ROUND * i; j < expectedSizeAfterThisRound; j++) {
                map.put(j, j);
            }
            assertTrueEventually(() -> {
                assertEquals(expectedSizeAfterThisRound, map.size());
            });
            assertTrueEventually(() -> {
                assertEquals(expectedSizeAfterThisRound, functionCall.get());
            });
            job.suspend();
            assertTrueEventually(() -> {
                assertEquals(JobStatus.SUSPENDED, job.getStatus());
            });
            job.resume();
            assertTrueEventually(() -> {
                assertEquals(JobStatus.RUNNING, job.getStatus());
            });
            assertEquals(expectedSizeAfterThisRound, functionCall.get());
        }
    }

}
