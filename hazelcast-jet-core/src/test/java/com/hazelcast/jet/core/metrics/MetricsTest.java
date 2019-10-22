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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private JetInstance instance;
    private Pipeline pipeline;

    @Before
    public void before() {
        instance = createJetMember();
        pipeline = Pipeline.create();
    }

    @Test
    public void counter_notUsed() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        Metrics.metric("dropped"); //retrieve "dropped" counter, but never use it
                    }
                    //not even retrieve "total" counter

                    return pass;
                })
                .drainTo(Sinks.logger());

        assertMetricsProduced(pipeline, "dropped", 0, "total", null);
    }

    @Test
    public void counter() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        Metrics.metric("dropped").inc();
                    }
                    Metrics.metric("total").inc();

                    return pass;
                })
                .drainTo(Sinks.logger());

        assertMetricsProduced(pipeline, "dropped", 2, "total", 5);
    }

    @Test
    public void gauge() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    Metric metric = Metrics.metric("sum", Unit.COUNT);
                    metric.set(acc.get());
                    return acc.get();
                })
                .drainTo(Sinks.logger());

        assertMetricsProduced(pipeline, "sum", 10);
    }

    @Test
    public void gauge_notUsed() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    Metrics.metric("sum", Unit.COUNT);
                    return acc.get();
                })
                .drainTo(Sinks.logger());

        assertMetricsProduced(pipeline, "sum", 0L);
    }

    @Test
    public void nonCooperativeProcessor() {
        DAG dag = new DAG();

        Vertex source = dag.newVertex("source", TestProcessors.ListSource.supplier(asList(1L, 2L, 3L)));
        Vertex map = dag.newVertex("map", new NonCoopTransformPSupplier((FunctionEx<Long, Long>) l -> {
            Metrics.metric("mapped").inc();
            return l * 10L;
        }));
        Vertex sink = dag.newVertex("sink", writeListP("results"));

        dag.edge(between(source, map)).edge(between(map, sink));

        assertMetricsProduced(dag, "mapped", 3L);
        assertEquals(
                new HashSet<>(Arrays.asList(10L, 20L, 30L)),
                new HashSet<>(instance.getList("results"))
        );
    }

    @Test
    public void metricsDisabled() {
        Long[] input = {0L, 1L, 2L, 3L, 4L};
        pipeline.drawFrom(TestSources.items(input))
                .map(l -> {
                    Metrics.metric("mapped").inc();
                    Metrics.metric("total", Unit.COUNT).set(input.length);
                    return l;
                })
                .drainTo(Sinks.logger());

        Job job = instance.newJob(pipeline, new JobConfig().setMetricsEnabled(false));
        job.join();

        JobMetrics metrics = job.getMetrics();
        assertTrue(metrics.get("mapped").isEmpty());
        assertTrue(metrics.get("total").isEmpty());
    }

    @Test
    public void usingContextAsync() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filterUsingServiceAsync(
                        ServiceFactory.withCreateFn(i -> 0L),
                        (ctx, l) -> {
                            Metric dropped = Metrics.metric("dropped");
                            Metric total = Metrics.metric("total");
                            return CompletableFuture.supplyAsync(
                                    () -> {
                                        boolean pass = l % 2L == ctx;
                                        if (!pass) {
                                            dropped.inc();
                                        }
                                        total.inc();
                                        return pass;
                                    }
                            );
                        }
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertMetricsProduced(pipeline, "dropped", 2L, "total", 4L);
    }

    private void assertMetricsProduced(Pipeline pipeline, Object... expected) {
        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        job.join();

        JobMetrics metrics = job.getMetrics();
        for (int i = 0; i < expected.length; i += 2) {
            String name = (String) expected[i];
            List<Measurement> measurements = metrics.get(name);
            assertMetricValue(name, measurements, expected[i + 1]);
        }
    }

    private void assertMetricsProduced(DAG dag, Object... expected) {
        Job job = instance.newJob(dag, JOB_CONFIG_WITH_METRICS);
        job.join();

        JobMetrics metrics = job.getMetrics();
        for (int i = 0; i < expected.length; i += 2) {
            String name = (String) expected[i];
            List<Measurement> measurements = metrics.get(name);
            assertMetricValue(name, measurements, expected[i + 1]);
        }
    }

    private void assertMetricValue(String name, List<Measurement> measurements, Object expected) {
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

    private static class NonCoopTransformPSupplier implements SupplierEx<Processor> {

        private final FunctionEx<Long, Long> mappingFn;

        NonCoopTransformPSupplier(FunctionEx<Long, Long> mappingFn) {
            this.mappingFn = mappingFn;
        }

        @Override
        public Processor getEx() {
            final ResettableSingletonTraverser<Long> trav = new ResettableSingletonTraverser<>();
            return new TransformP<Long, Long>(item -> {
                trav.accept(((FunctionEx<? super Long, ? extends Long>) mappingFn).apply(item));
                return trav;
            }) {
                @Override
                public boolean isCooperative() {
                    return false;
                }
            };
        }
    }

}
