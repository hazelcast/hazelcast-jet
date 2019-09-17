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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    @Test
    public void filter() {
        FilterWithUserMetrics filterFn = new FilterWithUserMetrics();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(TestSources.items(1, 2, 3, 4, 5))
                .filter(filterFn)
                .drainTo(Sinks.logger());

        JetInstance instance = createJetMember();
        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);

        job.join();
        JobMetrics metrics = job.getMetrics();

        List<Measurement> dropped = metrics.get("dropped");
        assertFalse(dropped.isEmpty());
        assertEquals(2L, dropped.get(0).getValue());

        List<Measurement> total = metrics.get("total");
        assertFalse(total.isEmpty());
        assertEquals(5L, total.get(0).getValue());
    }

    private static class FilterWithUserMetrics implements PredicateEx<Integer>, UserMetricsSource {

        @Probe
        private final AtomicLong dropped = new AtomicLong(); //user metric with field annotation

        private final AtomicLong total = new AtomicLong(); //user metric with method annotation

        @Override
        public boolean testEx(Integer anInt) throws Exception {
            boolean even = anInt % 2 == 0;
            if (even) {
                dropped.incrementAndGet();
            }
            total.incrementAndGet();
            return even;
        }

        @Probe
        public long getTotal() {
            return total.get();
        }

        @Nonnull
        @Override
        public List<Object> getMetricsSources() {
            return Collections.singletonList(this);
        }
    }
}
