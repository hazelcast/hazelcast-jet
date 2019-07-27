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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobMetrics;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static org.junit.Assert.assertEquals;


public class JobMetrics_StreamTest extends TestInClusterSupport {

    private static final String NOT_FILTER_OUT_PREFIX = "ok";
    private static final String FILTER_OUT_PREFIX = "nok";

    private static final String FLAT_MAP_AND_FILTER_VERTEX = "fused(map, filter)";
    private static final String RECEIVE_COUNT_METRIC = "receivedCount";
    private static final String EMITTED_COUNT_METRIC = "emittedCount";

    private static String journalMapName;
    private static String sinkListName;

    @Before
    public void before() {
        journalMapName = JOURNALED_MAP_PREFIX + randomString();
        sinkListName = "sinkList" + randomString();
    }

    @Test
    public void when_jobRunning_then_metricsEventuallyExist() {
        Map<String, String> map = testMode.getJet().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = testMode.getJet().getList(sinkListName);

        Pipeline p = createPipeline();
        // When
        Job job = testMode.getJet().newJob(p);

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(3, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));
    }

    @Test
    public void when_jobCancelled_then_terminalMetricsExist() {
        Map<String, String> map = testMode.getJet().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = testMode.getJet().getList(sinkListName);

        Pipeline p = createPipeline();
        Job job = testMode.getJet().newJob(p);

        putIntoMap(map, 1, 1);

        assertTrueEventually(() -> assertEquals(3, sink.size()));

        // When
        job.cancel();
        assertJobStatusEventually(job, FAILED);
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));
    }

    @Test
    @Ignore("https://hazelcast.atlassian.net/wiki/spaces/JET/pages/1758560997/"
            + "Metrics+for+Completed+Jobs+Design?focusedCommentId=1767506913#comment-1767506913")
    public void metricsExistWhenJobSuspendedAndResumed() {
        Map<String, String> map = testMode.getJet().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = testMode.getJet().getList(sinkListName);

        Pipeline p = createPipeline();

        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = testMode.getJet().newJob(p, jobConfig);

        putIntoMap(map, 1, 1);

        assertTrueEventually(() -> assertEquals(3, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        job.suspend();

        assertJobStatusEventually(job, SUSPENDED);
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        putIntoMap(map, 1, 1);
        assertTrueAllTheTime(() -> assertMetrics(job.getMetrics(), 5, 2), 5);

        job.resume();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(4, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 7, 3));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(5, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 9, 4));

        job.cancel();
        assertJobStatusEventually(job, FAILED);
        assertMetrics(job.getMetrics(), 9, 4);
    }

    @Test
    public void when_jobRestarted_then_metricsReset() {
        Map<String, String> map = testMode.getJet().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = testMode.getJet().getList(sinkListName);

        Pipeline p = createPipeline();
        Job job = testMode.getJet().newJob(p);

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        putIntoMap(map, 1, 1);

        assertTrueEventually(() -> assertEquals(3, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        // When
        job.restart();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(6, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(7, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 7, 3));

        job.cancel();
        assertJobStatusEventually(job, FAILED);
        assertMetrics(job.getMetrics(), 7, 3);
    }

    @Test
    public void when_jobRestarted_then_metricsReset_withJournal() {
        Map<String, String> map = testMode.getJet().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = testMode.getJet().getList(sinkListName);

        Pipeline p = createPipeline(JournalInitialPosition.START_FROM_CURRENT);
        Job job = testMode.getJet().newJob(p);

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 0, 0));

        putIntoMap(map, 2, 1);

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        // When
        job.restart();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(2, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 0, 0));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(3, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 2, 1));

        job.cancel();
        assertJobStatusEventually(job, FAILED);
        assertMetrics(job.getMetrics(), 2, 1);
    }

    @Test
    @Ignore("https://hazelcast.atlassian.net/wiki/spaces/JET/pages/1758560997/"
            + "Metrics+for+Completed+Jobs+Design?focusedCommentId=1767866594#comment-1767866594")
    public void notResetMetricsForExactlyOnceProcessingGuarantee() {
        Map<String, String> map = testMode.getJet().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = testMode.getJet().getList(sinkListName);

        Pipeline p = createPipeline(JournalInitialPosition.START_FROM_CURRENT);
        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = testMode.getJet().newJob(p, jobConfig);

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 0, 0));

        putIntoMap(map, 2, 1);

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        job.restart();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(2, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(3, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        job.cancel();
        assertJobStatusEventually(job, FAILED);
        assertMetrics(job.getMetrics(), 5, 2);
    }

    private Pipeline createPipeline() {
        return createPipeline(JournalInitialPosition.START_FROM_OLDEST);
    }

    private Pipeline createPipeline(JournalInitialPosition position) {
        Pipeline p = Pipeline.create();
        p.<Map.Entry<String, String>>drawFrom(Sources.mapJournal(journalMapName, position))
                .withIngestionTimestamps()
                .map(map -> map.getKey())
                .filter(word -> !word.startsWith(FILTER_OUT_PREFIX))
                .drainTo(Sinks.list(sinkListName));
        return p;
    }

    private void putIntoMap(Map<String, String> map, int notFilterOutItemsCount, int filterOutItemsCount) {
        for (int i = 0; i < notFilterOutItemsCount; i++) {
            map.put(NOT_FILTER_OUT_PREFIX + randomString(), "whateverHere");
        }
        for (int i = 0; i < filterOutItemsCount; i++) {
            map.put(FILTER_OUT_PREFIX + randomString(), "whateverHere");
        }
    }

    private void assertMetrics(JobMetrics metrics, int allItems, int filterOutItems) {
        Assert.assertNotNull(metrics);
        assertEquals(allItems, sumValueFor(metrics, "mapJournalSource(" + journalMapName + ")", EMITTED_COUNT_METRIC));
        assertEquals(allItems, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, RECEIVE_COUNT_METRIC));
        assertEquals(allItems - filterOutItems, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, EMITTED_COUNT_METRIC));
        assertEquals(allItems - filterOutItems,
                sumValueFor(metrics, "listSink(" + sinkListName + ")", RECEIVE_COUNT_METRIC));
    }

    private int sumValueFor(JobMetrics metrics, String vertex, String metric) {
        Set<String> metricNames = metrics.withTag("vertex", vertex).withTag("metric", metric).getMetricNames();
        int sum = 0;
        for (String metricName : metricNames) {
            if (!metricName.contains("ordinal=snapshot")) {
                sum += metrics.getMetricValue(metricName);
            }
        }
        return sum;
    }
}
