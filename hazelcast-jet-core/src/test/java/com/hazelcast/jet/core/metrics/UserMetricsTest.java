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
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private static final String DROPPED = "dropped";
    private static final String TOTAL = "total";

    private static final String MAPPED = "mapped";

    private Pipeline pipeline;

    @Before
    public void before() {
        pipeline = Pipeline.create();
    }

    @Test
    public void filter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new FilteringPredicateProvidingMetrics(i -> i % 2 == 0))
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 5);
    }

    @Test
    public void filter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filter(new FilteringPredicateProvidingMetrics(i -> i % 2 == 0))
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterUsingContext_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 0L),
                        new FilteringBiPredicateProvidingMetrics((ctx, l) -> l % 2 == ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 5);
    }

    @Test
    public void filterUsingContext_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 0L),
                        new FilteringTriPredicateProvidingMetrics((ctx, key, l) -> l % 2 == ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 5);
    }

    @Test
    public void filterUsingContext_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 0L),
                        new FilteringBiPredicateProvidingMetrics((ctx, l) -> l % 2 == ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterUsingContext_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 0L),
                        new FilteringTriPredicateProvidingMetrics((ctx, key, l) -> l % 2 == ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterUsingContextAsync_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 0L),
                        new AsyncFilterBiFunctionProvidingMetrics((ctx, l) -> l % 2L == ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 5);
    }

    @Test
    public void filterUsingContextAsync_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .filterUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 0L),
                        new AsyncFilterTriFunctionProvidingMetrics((ctx, key, l) -> l % 2L == ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 5);
    }

    @Test
    public void filterUsingContextAsync_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filterUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 0L),
                        new AsyncFilterBiFunctionProvidingMetrics((ctx, l) -> l % 2L == ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterUsingContextAsync_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .filterUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 0L),
                        new AsyncFilterTriFunctionProvidingMetrics((ctx, key, l) -> l % 2L == ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void map_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .map(new MappingFunctionProvidingMetrics(l -> l * 10))
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void map_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .map(new MappingFunctionProvidingMetrics(l -> l * 10))
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingContext_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new MappingBiFunctionProvidingMetrics((ctx, l) -> l * ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingContext_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .mapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new MappingTriFunctionProvidingMetrics((ctx, key, l) -> l * ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingContext_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .mapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new MappingBiFunctionProvidingMetrics((ctx, l) -> l * ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingContext_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .mapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new MappingTriFunctionProvidingMetrics((ctx, key, l) -> l * ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void fusedMapAndFilter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new FilteringPredicateProvidingMetrics(i -> i % 2 == 0))
                .map(new MappingFunctionProvidingMetrics(l -> l * 10))
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, MAPPED, 3, TOTAL, 5);
    }

    @Test
    public void fusedMapAndFilter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filter(new FilteringPredicateProvidingMetrics(i -> i % 2 == 0))
                .map(new MappingFunctionProvidingMetrics(l -> l * 10))
                .drainTo(assertAnyOrder(Arrays.asList(120L, 300L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4, MAPPED, 2);
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
            List<Measurement> measurements = metrics.get(name);
            assertCounterValue(name, measurements, (long) (Integer) expected[i + 1]);
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

    private abstract static class AbstractFiltering implements ProvidesMetrics, Serializable {
        private AtomicLong droppedCounter = new AtomicLong();
        private AtomicLong totalCounter = new AtomicLong();

        @Override
        public void registerMetrics(MetricsContext context) {
            context.registerGauge(DROPPED, droppedCounter::get);
            context.registerGauge(TOTAL, totalCounter::get);
        }

        void incCounters(boolean passed) {
            if (!passed) {
                droppedCounter.incrementAndGet();
            }
            totalCounter.incrementAndGet();
        }
    }

    private static class FilteringPredicateProvidingMetrics extends AbstractFiltering
            implements PredicateEx<Long>, ProvidesMetrics {

        private final PredicateEx<Long> predicate;

        FilteringPredicateProvidingMetrics(PredicateEx<Long> predicate) {
            this.predicate = predicate;
        }

        @Override
        public boolean testEx(Long aLong) {
            boolean pass = predicate.test(aLong);
            incCounters(pass);
            return pass;
        }
    }

    private static class FilteringBiPredicateProvidingMetrics extends AbstractFiltering
            implements BiPredicateEx<Long, Long>, ProvidesMetrics {

        private final BiPredicateEx<Long, Long> biPredicate;

        FilteringBiPredicateProvidingMetrics(BiPredicateEx<Long, Long> biPredicate) {
            this.biPredicate = biPredicate;
        }

        @Override
        public boolean testEx(Long t, Long u) throws Exception {
            boolean pass = biPredicate.testEx(t, u);
            incCounters(pass);
            return pass;
        }
    }

    private static class FilteringTriPredicateProvidingMetrics extends AbstractFiltering
            implements TriPredicate<Long, Long, Long>, ProvidesMetrics {

        private final TriPredicate<Long, Long, Long> triPredicate;

        FilteringTriPredicateProvidingMetrics(TriPredicate<Long, Long, Long> triPredicate) {
            this.triPredicate = triPredicate;
        }

        @Override
        public boolean testEx(Long t, Long u, Long v) throws Exception {
            boolean pass = triPredicate.testEx(t, u, v);
            incCounters(pass);
            return pass;
        }
    }

    private static class AsyncFilterBiFunctionProvidingMetrics extends AbstractFiltering
            implements BiFunctionEx<Long, Long, CompletableFuture<Boolean>>, ProvidesMetrics {

        private final BiFunctionEx<Long, Long, Boolean> biFunctionEx;

        AsyncFilterBiFunctionProvidingMetrics(BiFunctionEx<Long, Long, Boolean> biFunctionEx) {
            this.biFunctionEx = biFunctionEx;
        }

        @Override
        public CompletableFuture<Boolean> applyEx(Long ctx, Long l) {
            return CompletableFuture.supplyAsync(() -> {
                boolean pass = biFunctionEx.apply(ctx, l);
                incCounters(pass);
                return pass;
            });
        }
    }

    private static class AsyncFilterTriFunctionProvidingMetrics extends AbstractFiltering
            implements TriFunction<Long, Long, Long, CompletableFuture<Boolean>>, ProvidesMetrics {

        private final TriFunction<Long, Long, Long, Boolean> triFunction;

        AsyncFilterTriFunctionProvidingMetrics(TriFunction<Long, Long, Long, Boolean> triFunction) {
            this.triFunction = triFunction;
        }

        @Override
        public CompletableFuture<Boolean> applyEx(Long t0, Long t1, Long t2) {
            return CompletableFuture.supplyAsync(() -> {
                boolean pass = triFunction.apply(t0, t1, t2);
                incCounters(pass);
                return pass;
            });
        }
    }

    private abstract static class AbstractMapping implements ProvidesMetrics, Serializable {

        private Counter mappedCounter;

        @Override
        public void registerMetrics(MetricsContext context) {
            if (mappedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            mappedCounter = context.registerCounter(MAPPED);
        }

        void incCounter() {
            mappedCounter.increment();
        }
    }

    private static class MappingFunctionProvidingMetrics extends AbstractMapping
            implements FunctionEx<Long, Long>, ProvidesMetrics {

        private final FunctionEx<Long, Long> mappingFunction;

        MappingFunctionProvidingMetrics(FunctionEx<Long, Long> mappingFunction) {
            this.mappingFunction = mappingFunction;
        }

        @Override
        public Long applyEx(Long aLong) {
            incCounter();
            return mappingFunction.apply(aLong);
        }
    }

    private static class MappingBiFunctionProvidingMetrics extends AbstractMapping
            implements BiFunctionEx<Long, Long, Long>, ProvidesMetrics {

        private final BiFunctionEx<Long, Long, Long> mappingFunction;

        MappingBiFunctionProvidingMetrics(BiFunctionEx<Long, Long, Long> mappingFunction) {
            this.mappingFunction = mappingFunction;
        }

        @Override
        public Long applyEx(Long t, Long u) {
            incCounter();
            return mappingFunction.apply(t, u);
        }
    }

    private static class MappingTriFunctionProvidingMetrics extends AbstractMapping
            implements TriFunction<Long, Long, Long, Long>, ProvidesMetrics {

        private final TriFunction<Long, Long, Long, Long> mappingFunction;

        MappingTriFunctionProvidingMetrics(TriFunction<Long, Long, Long, Long> mappingFunction) {
            this.mappingFunction = mappingFunction;
        }

        @Override
        public Long applyEx(Long t0, Long t1, Long t2) {
            incCounter();
            return mappingFunction.apply(t0, t1, t2);
        }
    }

    private static class FlatMapProvidingMetrics implements FunctionEx<Long, Traverser<Long>>, ProvidesMetrics {

        private static final String EXPANDED = "expanded";

        private final FunctionEx<Long, Long[]> expandFn;

        private Counter expandedCounter;

        FlatMapProvidingMetrics(FunctionEx<Long, Long[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public void registerMetrics(MetricsContext context) {
            if (expandedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            expandedCounter = context.registerCounter(EXPANDED);
        }

        @Override
        public Traverser<Long> applyEx(Long aLong) {
            Long[] expansions = expandFn.apply(aLong);
            expandedCounter.increment(expansions.length);
            return Traversers.traverseItems(expansions);
        }
    }

    private static class AccumulateProvidingMetrics implements BiConsumerEx<LongAccumulator, Long>, ProvidesMetrics {

        private final String metricName;
        private Counter addedCounter;

        AccumulateProvidingMetrics(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public void acceptEx(LongAccumulator longAccumulator, Long l) {
            longAccumulator.add(l);
            addedCounter.increment();
        }

        @Override
        public void registerMetrics(MetricsContext context) {
            if (addedCounter != null) {
                throw new IllegalStateException("Should get initialised only once!");
            }
            addedCounter = context.registerCounter(metricName);
        }

    }
}
