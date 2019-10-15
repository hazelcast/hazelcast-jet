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
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UserMetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private static final String DROPPED = "dropped";
    private static final String TOTAL = "total";

    private JetInstance instance;
    private Pipeline pipeline;

    @Before
    public void before() {
        instance = createJetMember();
        pipeline = Pipeline.create();
    }

    @Test
    public void non_usage() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        UserMetrics.get(DROPPED); //retrieve "dropped" counter, but never use it
                    }
                    //not even retrieve "total" counter

                    return pass;
                })
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 0, TOTAL, null);
    }

    @Test
    public void filter() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        Counter dropped = UserMetrics.get(DROPPED);
                        dropped.increment();
                    }
                    UserMetrics.get(TOTAL).increment();

                    return pass;
                })
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 5);
    }

    private void assertCountersProduced(Object... expected) {
        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);

        job.join();

        JobMetrics metrics = job.getMetrics();
        for (int i = 0; i < expected.length; i += 2) {
            String name = (String) expected[i];
            List<Measurement> measurements = metrics.get(name);
            assertCounterValue(name, measurements, expected[i + 1]);
        }
    }

    private void assertCounterValue(String name, List<Measurement> measurements, Object expected) {
        if (expected == null) {
            assertTrue(
                    String.format("Did not expect measurements for metric '%s', but there were some", name),
                    measurements.isEmpty()
            );
        } else {
            assertFalse(
                    String.format("Expected measurements for metric '%s', but there were none", name),
                    measurements.isEmpty()
            );
            long actualValue = measurements.stream().mapToLong(Measurement::getValue).sum();
            if (expected instanceof Number) {
                long expectedValue = ((Number) expected).longValue();
                assertEquals(
                        String.format("Expected %d for metric '%s', but got %d instead", expectedValue, name,
                                actualValue),
                        expectedValue,
                        actualValue
                );
            } else {
                long expectedMinValue = ((long[]) expected)[0];
                long expectedMaxValue = ((long[]) expected)[1];
                assertTrue(
                        String.format("Expected a value in the range [%d, %d] for metric '%s', but got %d",
                                expectedMinValue, expectedMaxValue, name, actualValue),
                        expectedMinValue <= actualValue && actualValue <= expectedMaxValue
                );
            }
        }
    }

}
