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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    @Test
    public void filter() {
        FilterWithMetrics filterFn = new FilterWithMetrics();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(TestSources.items(1, 2, 3, 4, 5))
                .filter(filterFn)
                .drainTo(Sinks.logger());

        JetInstance instance = createJetMember();
        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);

        job.join();

        JobMetrics metrics = job.getMetrics();
        assertCounterValue(metrics.get("dropped"), 2L);
        assertCounterValue(metrics.get("total"), 5L);
    }

    private void assertCounterValue(List<Measurement> measurements, long expectedValue) {
        assertFalse(measurements.isEmpty());
        assertEquals(expectedValue, measurements.stream().mapToLong(Measurement::getValue).sum());
    }

    private static class FilterWithMetrics implements PredicateEx<Integer>, MetricsOperator {

        private AtomicLong droppedCounter;
        private AtomicLong totalCounter;

        @Override
        public void init(MetricsContext context) {
            droppedCounter = context.getCounter("dropped");
            totalCounter = context.getCounter("total");
        }

        @Override
        public boolean testEx(Integer anInt) {
            boolean even = anInt % 2 == 0;
            if (even) {
                droppedCounter.incrementAndGet();
            }

            totalCounter.incrementAndGet();

            return even;
        }
    }
}
