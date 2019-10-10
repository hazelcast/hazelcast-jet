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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
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

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetrics_FlatMapTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private static final String FLAT_MAPPED = "flat_mapped";
    private static final String EVICTED = "evicted";

    private JetInstance instance;
    private Pipeline pipeline;

    @Before
    public void before() {
        instance = createJetMember();
        pipeline = Pipeline.create();
    }

    @Test
    public void flatMap_batch() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .flatMap(new FlatMapFunctionProvidingMetrics<>(l -> new Long[]{l, l + 1}))
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMap_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .flatMap(new FlatMapFunctionProvidingMetrics<>(l -> new Long[]{l, l + 1}))
                .drainTo(assertAnyOrder(Arrays.asList(3L, 4L, 12L, 13L)));

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapUsingContext_batch() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new FlatMapBiFunctionProvidingMetrics<>((ctx, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMapUsingContext_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .groupingKey(l -> l % 3L)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new FlatMapTriFunctionProvidingMetrics<>((ctx, key, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMapUsingContext_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new FlatMapBiFunctionProvidingMetrics<>((ctx, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(assertAnyOrder(Arrays.asList(3L, 13L, 12L, 22L)));

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapUsingContext_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .flatMapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new FlatMapTriFunctionProvidingMetrics<>((ctx, key, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(assertAnyOrder(Arrays.asList(3L, 13L, 12L, 22L)));

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapUsingContextAsync_batch() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .flatMapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncFlatMapBiFunctionProvidingMetrics<>((ctx, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMapUsingContextAsync_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .groupingKey(l -> l % 3L)
                .flatMapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncFlatMapTriFunctionProvidingMetrics<>((ctx, key, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMapUsingContextAsync_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .flatMapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncFlatMapBiFunctionProvidingMetrics<>((ctx, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(assertAnyOrder(Arrays.asList(3L, 13L, 12L, 22L)));

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapUsingContextAsync_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .flatMapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncFlatMapTriFunctionProvidingMetrics<>((ctx, key, l) -> new Long[]{l, l + ctx})
                )
                .drainTo(assertAnyOrder(Arrays.asList(3L, 13L, 12L, 22L)));

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapStateful_batch() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .flatMapStateful(
                        LongAccumulator::new,
                        new FlatMapBiFunctionProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return new Long[]{l, l + acc.get()};
                        }))
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMapStateful_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 2L, 4L, 6L, 8L))
                .groupingKey(l -> l % 3L)
                .flatMapStateful(
                        LongAccumulator::new,
                        new FlatMapTriFunctionProvidingMetrics<>((acc, key, l) -> {
                            acc.add(1);
                            return new Long[]{l, l + acc.get()};
                        }))
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 10);
    }

    @Test
    public void flatMapStateful_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .flatMapStateful(
                        LongAccumulator::new,
                        new FlatMapBiFunctionProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return new Long[]{l, l + acc.get()};
                        }))
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapStateful_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .flatMapStateful(
                        LongAccumulator::new,
                        new FlatMapTriFunctionProvidingMetrics<>((acc, key, l) -> {
                            acc.add(1);
                            return new Long[]{l, l + acc.get()};
                        }))
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 4);
    }

    @Test
    public void flatMapStatefulWithEvict_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .flatMapStateful(
                        MINUTES.toMillis(1),
                        LongAccumulator::new,
                        new FlatMapTriFunctionProvidingMetrics<>((acc, key, l) -> {
                            acc.add(1);
                            return new Long[]{l, l + acc.get()};
                        }),
                        new EvictTriFunctionProvidingMetrics<>((counter, key, wm) -> Traversers.singleton(wm))
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FLAT_MAPPED, 4, EVICTED, 0);
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
                String.format("Expected measurements for metric '%s', but there were none", name),
                measurements.isEmpty()
        );
        long actualValue = measurements.stream().mapToLong(Measurement::getValue).sum();
        assertEquals(
                String.format("Expected %d for metric '%s', but got %d instead", expectedValue, name, actualValue),
                expectedValue,
                actualValue
        );
    }

    private abstract static class AbstractFlatMapping implements MetricsProvider, Serializable {
        private Counter expandedCounter;

        @Override
        public void registerMetrics(MetricsContext context) {
            if (expandedCounter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once");
            }
            expandedCounter = context.registerCounter(FLAT_MAPPED);
        }

        void incrementCounter(int increment) {
            expandedCounter.increment(increment);
        }
    }

    private static class FlatMapFunctionProvidingMetrics<T> extends AbstractFlatMapping
            implements FunctionEx<T, Traverser<T>> {
        private final FunctionEx<T, T[]> expandFn;

        FlatMapFunctionProvidingMetrics(FunctionEx<T, T[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public Traverser<T> applyEx(T t) {
            T[] expansion = expandFn.apply(t);
            incrementCounter(expansion.length);
            return Traversers.traverseItems(expansion);
        }
    }

    private static class FlatMapBiFunctionProvidingMetrics<T, U> extends AbstractFlatMapping
            implements BiFunctionEx<T, U, Traverser<U>> {
        private final BiFunctionEx<T, U, U[]> expandFn;

        FlatMapBiFunctionProvidingMetrics(BiFunctionEx<T, U, U[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public Traverser<U> applyEx(T t, U u) {
            U[] expansion = expandFn.apply(t, u);
            incrementCounter(expansion.length);
            return Traversers.traverseItems(expansion);
        }
    }

    private static class FlatMapTriFunctionProvidingMetrics<T, U, V> extends AbstractFlatMapping
            implements TriFunction<T, U, V, Traverser<V>> {
        private final TriFunction<T, U, V, V[]> expandFn;

        FlatMapTriFunctionProvidingMetrics(TriFunction<T, U, V, V[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public Traverser<V> applyEx(T t, U u, V v) {
            V[] expansion = expandFn.apply(t, u, v);
            incrementCounter(expansion.length);
            return Traversers.traverseItems(expansion);
        }
    }

    private static class AsyncFlatMapBiFunctionProvidingMetrics<T, U> extends AbstractFlatMapping
            implements BiFunctionEx<T, U, CompletableFuture<Traverser<U>>> {
        private final BiFunctionEx<T, U, U[]> expandFn;

        AsyncFlatMapBiFunctionProvidingMetrics(BiFunctionEx<T, U, U[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public CompletableFuture<Traverser<U>> applyEx(T t, U u) {
            return CompletableFuture.supplyAsync(() -> {
                U[] expansion = expandFn.apply(t, u);
                incrementCounter(expansion.length);
                return Traversers.traverseItems(expansion);
            });
        }
    }

    private static class AsyncFlatMapTriFunctionProvidingMetrics<T, U, V> extends AbstractFlatMapping
            implements TriFunction<T, U, V, CompletableFuture<Traverser<V>>> {
        private final TriFunction<T, U, V, V[]> expandFn;

        AsyncFlatMapTriFunctionProvidingMetrics(TriFunction<T, U, V, V[]> expandFn) {
            this.expandFn = expandFn;
        }

        @Override
        public CompletableFuture<Traverser<V>> applyEx(T t, U u, V v) {
            return CompletableFuture.supplyAsync(() -> {
                V[] expansion = expandFn.apply(t, u, v);
                incrementCounter(expansion.length);
                return Traversers.traverseItems(expansion);
            });
        }
    }

    private abstract static class AbstractEviction implements MetricsProvider, Serializable {

        private Counter evictedCounter;

        @Override
        public void registerMetrics(MetricsContext context) {
            if (evictedCounter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once");
            }
            evictedCounter = context.registerCounter(EVICTED);
        }

        void incCounter() {
            evictedCounter.increment();
        }
    }

    private static class EvictTriFunctionProvidingMetrics<T, U, V> extends AbstractEviction
            implements TriFunction<T, U, Long, Traverser<V>> {
        private final TriFunction<T, U, Long, Traverser<V>> evictFunction;

        EvictTriFunctionProvidingMetrics(TriFunction<T, U, Long, Traverser<V>> evictFunction) {
            this.evictFunction = evictFunction;
        }

        @Override
        public Traverser<V> applyEx(T t, U u, Long wm) {
            Traverser<V> traverser = evictFunction.apply(t, u, wm);
            incCounter();
            return traverser;
        }
    }

}
