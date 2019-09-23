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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private Pipeline pipeline;

    @Before
    public void before() {
        pipeline = Pipeline.create();
    }

    @Test
    public void filter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .drainTo(Sinks.logger());

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                PredicateProvidingMetrics.TOTAL, 5);
    }

    @Test
    public void filter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                PredicateProvidingMetrics.TOTAL, 4);
    }

    @Test
    public void map_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .map(new MapProvidingMetrics())
                .drainTo(Sinks.logger());

        assertCountersProduced(MapProvidingMetrics.MAPPED, 5);
    }

    @Test
    public void map_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .map(new MapProvidingMetrics())
                .drainTo(assertAnyOrder(Arrays.asList(3L, 12L, 21L, 30L)));

        assertCountersProduced(MapProvidingMetrics.MAPPED, 4);
    }

    @Test
    public void fusedMapAndFilter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .map(new MapProvidingMetrics())
                .drainTo(Sinks.logger());

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                MapProvidingMetrics.MAPPED, 3,
                PredicateProvidingMetrics.TOTAL, 5);
    }

    @Test
    public void fusedMapAndFilter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .map(new MapProvidingMetrics())
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                PredicateProvidingMetrics.TOTAL, 4,
                MapProvidingMetrics.MAPPED, 2);
    }

    @Test
    public void flatMap_batch() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .flatMap(new FlatMapProvidingMetrics(l -> new Long[]{l, l + 1}))
                .drainTo(Sinks.logger());

        assertCountersProduced(FlatMapProvidingMetrics.EXPANDED, 10);
    }

    @Test
    public void flatMap_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .flatMap(new FlatMapProvidingMetrics(l -> new Long[]{l, l + 1}))
                .drainTo(assertAnyOrder(Arrays.asList(3L, 4L, 12L, 13L)));

        assertCountersProduced(FlatMapProvidingMetrics.EXPANDED, 4);
    }

    @Test
    public void aggregate1() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp = AggregateOperations.summingLong((Long x) -> x)
                                                    .withAccumulateFn(new AccumulateProvidingMetrics("added"));
        pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .aggregate(aggrOp)
                .drainTo(assertOrdered(Collections.singletonList(15L)));

        assertCountersProduced("added", 5);
    }

    @Test
    public void aggregate2() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp1 = AggregateOperations.summingLong((Long x) -> x)
                .withAccumulateFn(new AccumulateProvidingMetrics("added1"));
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp2 = AggregateOperations.summingLong((Long x) -> x)
                .withAccumulateFn(new AccumulateProvidingMetrics("added2"));

        BatchStage<Long> sourceStage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L));
        BatchStage<Long> sourceStage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L));
        sourceStage1.aggregate2(aggrOp1, sourceStage2, aggrOp2)
                .flatMap(t2 -> Traversers.traverseItems(t2.f0(), t2.f1()))
                .drainTo(assertOrdered(Arrays.asList(15L, 60L)));

        assertCountersProduced("added1", 5, "added2", 3);
    }

    private void assertCountersProduced(Object... expected) {
        JetInstance instance = createJetMember();
        Job job = instance.newJob(pipeline, JOB_CONFIG_WITH_METRICS);

        job.join();

        JobMetrics metrics = job.getMetrics();
        for (int i = 0; i < expected.length; i += 2) {
            String name = (String) expected[i];
            assertCounterValue(name, metrics.get(name), (long) (Integer) expected[i + 1]);
        }
    }

    private void assertCounterValue(String name, List<Measurement> measurements, long expectedValue) {
        assertFalse(measurements.isEmpty());
        long actualValue = measurements.stream().mapToLong(Measurement::getValue).sum();
        assertEquals(
                String.format("Expected %d for metric '%s', but got %d instead!", expectedValue, name, actualValue),
                expectedValue,
                actualValue
        );
    }

    private static class AccumulateProvidingMetrics implements BiConsumerEx<LongAccumulator, Long>, ProvidesMetrics {

        private final String metricName;
        private UserMetric addedCounter;

        AccumulateProvidingMetrics(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public void acceptEx(LongAccumulator longAccumulator, Long l) {
            longAccumulator.add(l);
            addedCounter.incValue();
        }

        @Override
        public void init(MetricsContext context) {
            if (addedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            addedCounter = context.getUserMetric(metricName);
        }

    }

    private static class PredicateProvidingMetrics implements PredicateEx<Long>, ProvidesMetrics {

        private static final String DROPPED = "dropped";
        private static final String TOTAL = "total";

        private final PredicateEx<Long> predicate;

        private AtomicLong droppedCounter = new AtomicLong();
        private AtomicLong totalCounter = new AtomicLong();

        PredicateProvidingMetrics(PredicateEx<Long> predicate) {
            this.predicate = predicate;
        }

        @Override
        public void init(MetricsContext context) {
            context.setUserMetricSupplier(DROPPED, droppedCounter::get);
            context.setUserMetricSupplier(TOTAL, totalCounter::get);
        }

        @Override
        public boolean testEx(Long aLong) {
            boolean pass = predicate.test(aLong);
            if (!pass) {
                droppedCounter.incrementAndGet();
            }

            totalCounter.incrementAndGet();

            return pass;
        }
    }

    private static class MapProvidingMetrics implements FunctionEx<Long, Long>, ProvidesMetrics {

        private static final String MAPPED = "mapped";

        private UserMetric mappedCounter;

        @Override
        public void init(MetricsContext context) {
            if (mappedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            mappedCounter = context.getUserMetric(MAPPED);
        }

        @Override
        public Long applyEx(Long aLong) {
            mappedCounter.incValue();
            return aLong;
        }
    }

    private static class FlatMapProvidingMetrics implements FunctionEx<Long, Traverser<Long>>, ProvidesMetrics {

        private static final String EXPANDED = "expanded";

        private final FunctionEx<Long, Long[]> expandFn;

        private UserMetric expandedCounter;

        FlatMapProvidingMetrics(FunctionEx<Long, Long[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public void init(MetricsContext context) {
            if (expandedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            expandedCounter = context.getUserMetric(EXPANDED);
        }

        @Override
        public Traverser<Long> applyEx(Long aLong) {
            Long[] expansions = expandFn.apply(aLong);
            expandedCounter.incValue(expansions.length);
            return Traversers.traverseItems(expansions);
        }
    }
}
