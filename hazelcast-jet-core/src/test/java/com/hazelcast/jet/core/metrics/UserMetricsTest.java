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
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4))
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .drainTo(Sinks.logger());

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                PredicateProvidingMetrics.TOTAL, 5);
    }

    @Test
    public void filter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4))
                .addTimestamps(o -> 0L, 0L)
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .drainTo(Sinks.logger());

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                PredicateProvidingMetrics.TOTAL, 5);
    }

    @Test
    public void map_batch() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4))
                .map(new MapProvidingMetrics())
                .drainTo(Sinks.logger());

        assertCountersProduced(MapProvidingMetrics.MAPPED, 5);
    }

    @Test
    public void map_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4))
                .addTimestamps(o -> 0L, 0L)
                .map(new MapProvidingMetrics())
                .drainTo(Sinks.logger());

        assertCountersProduced(MapProvidingMetrics.MAPPED, 5);
    }

    @Test
    public void fusedMapAndFilter_batch() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4))
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .map(new MapProvidingMetrics())
                .drainTo(Sinks.logger());

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                MapProvidingMetrics.MAPPED, 3,
                PredicateProvidingMetrics.TOTAL, 5);
    }

    @Test
    public void fusedMapAndFilter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4))
                .addTimestamps(o -> 0L, 0L)
                .filter(new PredicateProvidingMetrics(i -> i % 2 == 0))
                .map(new MapProvidingMetrics())
                .drainTo(Sinks.logger());

        assertCountersProduced(PredicateProvidingMetrics.DROPPED, 2,
                MapProvidingMetrics.MAPPED, 3,
                PredicateProvidingMetrics.TOTAL, 5);
    }

    @Test
    public void flatMap_batch() {
        pipeline.drawFrom(TestSources.items(0, 2, 4, 6, 8))
                .flatMap(new FlatMapProvidingMetrics(i -> new Integer[]{i, i + 1}))
                .drainTo(Sinks.logger());

        assertCountersProduced(FlatMapProvidingMetrics.EXPANDED, 10);
    }

    @Test
    public void flatMap_stream() {
        pipeline.drawFrom(TestSources.items(0, 2, 4, 6, 8))
                .addTimestamps(o -> 0L, 0L)
                .flatMap(new FlatMapProvidingMetrics(i -> new Integer[]{i, i + 1}))
                .drainTo(Sinks.logger());

        assertCountersProduced(FlatMapProvidingMetrics.EXPANDED, 10);
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
            assertCounterValue(metrics.get((String) expected[i]), (long) (Integer) expected[i + 1]);
        }
    }

    private void assertCounterValue(List<Measurement> measurements, long expectedValue) {
        assertFalse(measurements.isEmpty());
        assertEquals(expectedValue, measurements.stream().mapToLong(Measurement::getValue).sum());
    }

    private static class AccumulateProvidingMetrics implements BiConsumerEx<LongAccumulator, Long>, ProvidesMetrics {

        private final String metricName;
        private AtomicLong addedCounter;

        AccumulateProvidingMetrics(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public void acceptEx(LongAccumulator longAccumulator, Long l) throws Exception {
            longAccumulator.add(l);
            addedCounter.incrementAndGet();
        }

        @Override
        public void init(MetricsContext context) {
            if (addedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            addedCounter = context.getCounter(metricName);
        }

    }

    private static class PredicateProvidingMetrics implements PredicateEx<Integer>, ProvidesMetrics {

        private static final String DROPPED = "dropped";
        private static final String TOTAL = "total";

        private final PredicateEx<Integer> predicate;

        private AtomicLong droppedCounter;
        private AtomicLong totalCounter;

        PredicateProvidingMetrics(PredicateEx<Integer> predicate) {
            this.predicate = predicate;
        }

        @Override
        public void init(MetricsContext context) {
            if (droppedCounter != null || totalCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            droppedCounter = context.getCounter(DROPPED);
            totalCounter = context.getCounter(TOTAL);
        }

        @Override
        public boolean testEx(Integer anInt) {
            boolean pass = predicate.test(anInt);
            if (!pass) {
                droppedCounter.incrementAndGet();
            }

            totalCounter.incrementAndGet();

            return pass;
        }
    }

    private static class MapProvidingMetrics implements FunctionEx<Integer, Integer>, ProvidesMetrics {

        private static final String MAPPED = "mapped";

        private AtomicLong mappedCounter;

        @Override
        public void init(MetricsContext context) {
            if (mappedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            mappedCounter = context.getCounter(MAPPED);
        }

        @Override
        public Integer applyEx(Integer i) {
            mappedCounter.incrementAndGet();
            return i;
        }
    }

    private static class FlatMapProvidingMetrics implements FunctionEx<Integer, Traverser<Integer>>, ProvidesMetrics {

        private static final String EXPANDED = "expanded";

        private final FunctionEx<Integer, Integer[]> expandFn;

        private AtomicLong expandedCounter;

        FlatMapProvidingMetrics(FunctionEx<Integer, Integer[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public void init(MetricsContext context) {
            if (expandedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            expandedCounter = context.getCounter(EXPANDED);
        }

        @Override
        public Traverser<Integer> applyEx(Integer i) {
            Integer[] expansions = expandFn.apply(i);
            expandedCounter.addAndGet(expansions.length);
            return Traversers.traverseItems(expansions);
        }
    }
}
