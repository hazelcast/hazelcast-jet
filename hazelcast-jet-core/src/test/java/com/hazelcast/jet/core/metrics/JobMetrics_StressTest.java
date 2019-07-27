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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobMetrics;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class JobMetrics_StressTest extends JetTestSupport {

    private static final int RESTART_COUNT = 10;
    private static final int TOTAL_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final int WAIT_FOR_METRICS_COLLECTION_TIME = 1200;

    private static volatile Throwable restartThreadException;
    private static volatile Throwable obtainMetricsThreadException;

    private JetInstance instance;

    @Before
    public void setup() {
        restartThreadException = null;
        obtainMetricsThreadException = null;
        IncrementingProcessor.initCount.set(0);
        IncrementingProcessor.completeCount.set(0);

        JetConfig config = new JetConfig();
        config.getMetricsConfig().setCollectionIntervalSeconds(1);
        instance = createJetMember(config);
    }

    @Test
    public void restart_stressTest() throws Throwable {
        stressTest(JobRestartThread::new);
    }

    @Test
    public void suspend_resume_stressTest() throws Throwable {
        stressTest(JobSuspendResumeThread::new);
    }

    private void stressTest(Function<Job, Runnable> restart) throws Throwable {
        DAG dag = buildDag();
        Job job = instance.newJob(dag);
        try {
            assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));

            ObtainMetricsThread obtainMetrics = new ObtainMetricsThread(job);
            Thread restartThread = new Thread(restart.apply(job));
            Thread obtainThread = new Thread(obtainMetrics);

            restartThread.start();
            obtainThread.start();

            restartThread.join();
            obtainMetrics.stop = true;
            obtainThread.join();

            if (restartThreadException != null) {
                throw restartThreadException;
            }
            if (obtainMetricsThreadException != null) {
                throw obtainMetricsThreadException;
            }
        } finally {
            super.ditchJob(job, instance);
        }
    }

    private DAG buildDag() {
        DAG dag = new DAG();
        dag.newVertex("p", (SupplierEx<Processor>) IncrementingProcessor::new);
        return dag;
    }

    private static final class IncrementingProcessor implements Processor {

        @Probe
        static final AtomicInteger initCount = new AtomicInteger();

        @Probe
        static final AtomicInteger completeCount = new AtomicInteger();

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            initCount.incrementAndGet();
        }

        @Override
        public boolean complete() {
            completeCount.incrementAndGet();
            return false;
        }
    }

    private static class JobRestartThread implements Runnable {

        private final Job job;
        private int restartCount;

        JobRestartThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (restartCount < RESTART_COUNT) {
                    job.restart();
                    sleepMillis(WAIT_FOR_METRICS_COLLECTION_TIME);
                    restartCount++;
                    assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
                }
            } catch (Throwable ex) {
                restartThreadException = ex;
                throw ex;
            }
        }
    }

    private static class JobSuspendResumeThread implements Runnable {

        private final Job job;
        private int restartCount;

        JobSuspendResumeThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (restartCount < RESTART_COUNT) {
                    job.suspend();
                    assertTrueEventually(() -> assertEquals(JobStatus.SUSPENDED, job.getStatus()));
                    sleepMillis(WAIT_FOR_METRICS_COLLECTION_TIME);
                    job.resume();
                    restartCount++;
                    assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
                }
            } catch (Throwable ex) {
                restartThreadException = ex;
                throw ex;
            }
        }
    }

    private static class ObtainMetricsThread implements Runnable {

        volatile boolean stop;
        private final Job job;

        ObtainMetricsThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                long previousInitCountSum = 0;
                long previousCompleteCountSum = 0;
                while (!stop) {
                    assertNotNull(job.getMetrics());

                    JobMetrics withInitCountTag = job.getMetrics().withTag("metric", "initCount");
                    Set<String> initCountNames = withInitCountTag.getMetricNames();
                    if (initCountNames.size() != TOTAL_PROCESSORS) {
                        continue;
                    }
                    long initCountSum = sumValuesInMetrics(withInitCountTag, initCountNames);
                    assertTrue("Metrics value should be increasing, current: " + initCountSum
                            + ", previous: " + previousInitCountSum,
                            initCountSum >= previousInitCountSum);
                    previousInitCountSum = initCountSum;

                    JobMetrics withCompleteCountTag = job.getMetrics().withTag("metric", "completeCount");
                    Set<String> completeCountNames = withCompleteCountTag.getMetricNames();
                    if (completeCountNames.size() != TOTAL_PROCESSORS) {
                        continue;
                    }
                    long completeCountSum = sumValuesInMetrics(withCompleteCountTag, completeCountNames);
                    assertTrue("Metrics value should be increasing, current: " + completeCountSum
                            + ", previous: " + previousCompleteCountSum,
                            completeCountSum >= previousCompleteCountSum);
                    previousCompleteCountSum = completeCountSum;
                }
                assertTrueEventually(() -> {
                    assertNotNull(job.getMetrics());
                    JobMetrics withInitCountTag = job.getMetrics().withTag("metric", "initCount");
                    Set<String> metricNames = withInitCountTag.getMetricNames();
                    assertEquals(TOTAL_PROCESSORS, metricNames.size());
                    long sum = sumValuesInMetrics(withInitCountTag, metricNames);
                    assertEquals((RESTART_COUNT + 1) * TOTAL_PROCESSORS * TOTAL_PROCESSORS, sum);
                }, 3);
            } catch (Throwable ex) {
                obtainMetricsThreadException = ex;
                throw ex;
            }
        }

        private long sumValuesInMetrics(JobMetrics metrics, Set<String> metricNames) {
            long sum = 0;
            for (String metricName : metricNames) {
                sum += metrics.getMetricValue(metricName);
            }
            return sum;
        }
    }
}
