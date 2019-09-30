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
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetrics_FilterTest extends JetTestSupport {

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
    public void filter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new FilteringPredicateProvidingMetrics<>(l -> l % 2 == 0))
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
                .filter(new FilteringPredicateProvidingMetrics<>(i -> i % 2 == 0))
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterUsingContext_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 0L),
                        new FilteringBiPredicateProvidingMetrics<>((ctx, l) -> l % 2 == ctx)
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
                        new FilteringTriPredicateProvidingMetrics<>((ctx, key, l) -> l % 2 == ctx)
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
                        new FilteringBiPredicateProvidingMetrics<>((ctx, l) -> l % 2 == ctx)
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
                        new FilteringTriPredicateProvidingMetrics<>((ctx, key, l) -> l % 2 == ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterUsingContextAsync_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 0L),
                        new AsyncFilterBiFunctionProvidingMetrics<>((ctx, l) -> l % 2L == ctx)
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
                        new AsyncFilterTriFunctionProvidingMetrics<>((ctx, key, l) -> l % 2L == ctx)
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
                        new AsyncFilterBiFunctionProvidingMetrics<>((ctx, l) -> l % 2L == ctx)
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
                        new AsyncFilterTriFunctionProvidingMetrics<>((ctx, key, l) -> l % 2L == ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(12L, 30L)));

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterStateful_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterStateful(
                        LongAccumulator::new,
                        new FilteringBiPredicateProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return acc.get() % 2 == 0;
                        }))
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 3, TOTAL, 5);
    }

    @Test
    public void filterStateful_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .filterStateful(
                        LongAccumulator::new,
                        new FilteringBiPredicateProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return acc.get() % 2 == 0;
                        }))
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 3, TOTAL, 5);
    }

    @Test
    public void filterStateful_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filterStateful(
                        LongAccumulator::new,
                        new FilteringBiPredicateProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return acc.get() % 2 == 0;
                        })
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    @Test
    public void filterStateful_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .filterStateful(
                        LongAccumulator::new,
                        new FilteringBiPredicateProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return acc.get() % 2 == 0;
                        })
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(DROPPED, 2, TOTAL, 4);
    }

    private void assertCountersProduced(Object... expected) {
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
        assertFalse(
                String.format("Expected measurements for metric '%s', but there were none!", name),
                measurements.isEmpty()
        );
        long actualValue = measurements.stream().mapToLong(Measurement::getValue).sum();
        assertEquals(
                String.format("Expected %d for metric '%s', but got %d instead!", expectedValue, name, actualValue),
                expectedValue,
                actualValue
        );
    }

    private abstract static class AbstractFiltering implements ProvidesMetrics, Serializable {
        private Counter droppedCounter;
        private AtomicLong totalCounter = new AtomicLong();

        @Override
        public void registerMetrics(MetricsContext context) {
            if (droppedCounter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once!");
            }
            droppedCounter = context.registerCounter(DROPPED);

            context.registerGauge(TOTAL, totalCounter::get);
        }

        void incCounters(boolean passed) {
            if (!passed) {
                droppedCounter.increment();
            }
            totalCounter.incrementAndGet();
        }
    }

    private static class FilteringPredicateProvidingMetrics<T> extends AbstractFiltering
            implements PredicateEx<T> {

        private final PredicateEx<T> predicate;

        FilteringPredicateProvidingMetrics(PredicateEx<T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public boolean testEx(T t) {
            boolean pass = predicate.test(t);
            incCounters(pass);
            return pass;
        }
    }

    private static class FilteringBiPredicateProvidingMetrics<T, U> extends AbstractFiltering
            implements BiPredicateEx<T, U> {

        private final BiPredicateEx<T, U> biPredicate;

        FilteringBiPredicateProvidingMetrics(BiPredicateEx<T, U> biPredicate) {
            this.biPredicate = biPredicate;
        }

        @Override
        public boolean testEx(T t, U u) throws Exception {
            boolean pass = biPredicate.testEx(t, u);
            incCounters(pass);
            return pass;
        }
    }

    private static class FilteringTriPredicateProvidingMetrics<T, U, V> extends AbstractFiltering
            implements TriPredicate<T, U, V> {

        private final TriPredicate<T, U, V> triPredicate;

        FilteringTriPredicateProvidingMetrics(TriPredicate<T, U, V> triPredicate) {
            this.triPredicate = triPredicate;
        }

        @Override
        public boolean testEx(T t, U u, V v) throws Exception {
            boolean pass = triPredicate.testEx(t, u, v);
            incCounters(pass);
            return pass;
        }
    }

    private static class AsyncFilterBiFunctionProvidingMetrics<T, U> extends AbstractFiltering
            implements BiFunctionEx<T, U, CompletableFuture<Boolean>> {

        private final BiFunctionEx<T, U, Boolean> biFunctionEx;

        AsyncFilterBiFunctionProvidingMetrics(BiFunctionEx<T, U, Boolean> biFunctionEx) {
            this.biFunctionEx = biFunctionEx;
        }

        @Override
        public CompletableFuture<Boolean> applyEx(T t, U u) {
            return CompletableFuture.supplyAsync(() -> {
                boolean pass = biFunctionEx.apply(t, u);
                incCounters(pass);
                return pass;
            });
        }
    }

    private static class AsyncFilterTriFunctionProvidingMetrics<T, U, V> extends AbstractFiltering
            implements TriFunction<T, U, V, CompletableFuture<Boolean>> {

        private final TriFunction<T, U, V, Boolean> triFunction;

        AsyncFilterTriFunctionProvidingMetrics(TriFunction<T, U, V, Boolean> triFunction) {
            this.triFunction = triFunction;
        }

        @Override
        public CompletableFuture<Boolean> applyEx(T T, U u, V v) {
            return CompletableFuture.supplyAsync(() -> {
                boolean pass = triFunction.apply(T, u, v);
                incCounters(pass);
                return pass;
            });
        }
    }

}
