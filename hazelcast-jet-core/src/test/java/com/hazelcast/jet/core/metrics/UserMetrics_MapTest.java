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

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
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

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetrics_MapTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private static final String MAPPED = "mapped";
    private static final String EVICTED = "evicted";

    private JetInstance instance;
    private Pipeline pipeline;

    @Before
    public void before() {
        instance = createJetMember();
        pipeline = Pipeline.create();
    }

    @Test
    public void map_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .map(new MappingFunctionProvidingMetrics<>(l -> l * 10))
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
                .map(new MappingFunctionProvidingMetrics<>(l -> l * 10))
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingContext_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapUsingContext(
                        ContextFactory.withCreateFn(i -> 10L),
                        new MappingBiFunctionProvidingMetrics<>((ctx, l) -> l * ctx)
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
                        new MappingTriFunctionProvidingMetrics<>((ctx, key, l) -> l * ctx)
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
                        new MappingBiFunctionProvidingMetrics<>((ctx, l) -> l * ctx)
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
                        new MappingTriFunctionProvidingMetrics<>((ctx, key, l) -> l * ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingContextAsync_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncMappingBiFunctionProvidingMetrics<>((ctx, l) -> l * ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingContextAsync_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .mapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncMappingTriFunctionProvidingMetrics((ctx, key, l) -> l * ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingContextAsync_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .mapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncMappingBiFunctionProvidingMetrics<>((ctx, l) -> l * ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingContextAsync_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .mapUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 10L),
                        new AsyncMappingTriFunctionProvidingMetrics((ctx, key, l) -> l * ctx)
                )
                .drainTo(assertAnyOrder(Arrays.asList(30L, 120L, 210L, 300L)));

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapStateful_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(
                        LongAccumulator::new,
                        new MappingBiFunctionProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return l * acc.get();
                        })
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapStateful_batchWithKey() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .mapStateful(
                        LongAccumulator::new,
                        new MappingTriFunctionProvidingMetrics<>((acc, key, l) -> {
                            acc.add(1);
                            return l * acc.get() * key;
                        })
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapStateful_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .mapStateful(
                        LongAccumulator::new,
                        new MappingBiFunctionProvidingMetrics<>((acc, l) -> {
                            acc.add(1);
                            return l * acc.get();
                        })
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapStateful_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .mapStateful(
                        LongAccumulator::new,
                        new MappingTriFunctionProvidingMetrics<>((acc, key, l) -> {
                            acc.add(1);
                            return l * acc.get() * key;
                        })
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapStatefulWithEvict_streamWithKey() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .mapStateful(
                        MINUTES.toMillis(1),
                        LongAccumulator::new,
                        new MappingTriFunctionProvidingMetrics<>((acc, key, l) -> {
                            acc.add(1);
                            return l * acc.get() * key;
                        }),
                        new EvictTriFunctionProvidingMetrics<>((sum, key, time) ->
                                entry(String.format("%s:totalForSession:%d", key, time), sum.get())
                        ))
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 4, EVICTED, 0);
    }

    @Test
    public void mapUsingIMap_batch() {
        IMapJet<Long, String> map = instance.getMap("map");
        map.put(0L, "Zero");
        map.put(2L, "Two");
        map.put(4L, "Four");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapUsingIMap("map", FunctionEx.identity(),
                        new MappingBiFunctionProvidingMetrics<>(Tuple2::tuple2)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingIMap_batchWithKey() {
        IMapJet<Long, String> map = instance.getMap("map");
        map.put(0L, "Zero");
        map.put(2L, "Two");
        map.put(4L, "Four");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .groupingKey(l -> l % 3L)
                .mapUsingIMap("map",
                        new MappingBiFunctionProvidingMetrics<>(Tuple2::tuple2)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingIMap_stream() {
        IMapJet<Long, String> map = instance.getMap("map");
        map.put(0L, "Zero");
        map.put(2L, "Two");
        map.put(4L, "Four");

        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .mapUsingIMap(
                        "map",
                        FunctionEx.identity(),
                        new MappingBiFunctionProvidingMetrics<>(Tuple2::tuple2))
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingIMap_streamWithKey() {
        IMapJet<Long, String> map = instance.getMap("map");
        map.put(0L, "Zero");
        map.put(2L, "Two");
        map.put(4L, "Four");

        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .groupingKey(l -> l % 3L)
                .mapUsingIMap(
                        "map",
                        new MappingBiFunctionProvidingMetrics<>(Tuple2::tuple2))
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 4);
    }

    @Test
    public void mapUsingReplicatedMap_batch() {
        ReplicatedMap<Long, String> map = instance.getReplicatedMap("map");
        map.put(0L, "Zero");
        map.put(2L, "Two");
        map.put(4L, "Four");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapUsingReplicatedMap("map", FunctionEx.identity(),
                        new MappingBiFunctionProvidingMetrics<>(Tuple2::tuple2)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 5);
    }

    @Test
    public void mapUsingReplicatedMap_stream() {
        ReplicatedMap<Long, String> map = instance.getReplicatedMap("map");
        map.put(0L, "Zero");
        map.put(2L, "Two");
        map.put(4L, "Four");

        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .mapUsingReplicatedMap(
                        "map",
                        FunctionEx.identity(),
                        new MappingBiFunctionProvidingMetrics<>(Tuple2::tuple2))
                .drainTo(Sinks.logger());

        assertCountersProduced(MAPPED, 4);
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

    private abstract static class AbstractMapping implements MetricsProvider, Serializable {

        private Counter mappedCounter;

        @Override
        public void registerMetrics(MetricsContext context) {
            if (mappedCounter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once");
            }
            mappedCounter = context.registerCounter(MAPPED);
        }

        void incCounter() {
            mappedCounter.increment();
        }
    }

    private static class MappingFunctionProvidingMetrics<T, U> extends AbstractMapping
            implements FunctionEx<T, U> {

        private final FunctionEx<T, U> mappingFunction;

        MappingFunctionProvidingMetrics(FunctionEx<T, U> mappingFunction) {
            this.mappingFunction = mappingFunction;
        }

        @Override
        public U applyEx(T t) {
            incCounter();
            return mappingFunction.apply(t);
        }
    }

    private static class MappingBiFunctionProvidingMetrics<T, U, V> extends AbstractMapping
            implements BiFunctionEx<T, U, V> {

        private final BiFunctionEx<T, U, V> mappingFunction;

        MappingBiFunctionProvidingMetrics(BiFunctionEx<T, U, V> mappingFunction) {
            this.mappingFunction = mappingFunction;
        }

        @Override
        public V applyEx(T t, U u) {
            incCounter();
            return mappingFunction.apply(t, u);
        }
    }

    private static class MappingTriFunctionProvidingMetrics<T, U, V, R> extends AbstractMapping
            implements TriFunction<T, U, V, R> {

        private final TriFunction<T, U, V, R> mappingFunction;

        MappingTriFunctionProvidingMetrics(TriFunction<T, U, V, R> mappingFunction) {
            this.mappingFunction = mappingFunction;
        }

        @Override
        public R applyEx(T t, U u, V v) {
            incCounter();
            return mappingFunction.apply(t, u, v);
        }
    }

    private static class AsyncMappingBiFunctionProvidingMetrics<T, U> extends AbstractMapping
            implements BiFunctionEx<T, U, CompletableFuture<U>> {
        private final BiFunctionEx<T, U, U> biFunctionEx;

        AsyncMappingBiFunctionProvidingMetrics(BiFunctionEx<T, U, U> biFunctionEx) {
            this.biFunctionEx = biFunctionEx;
        }

        @Override
        public CompletableFuture<U> applyEx(T t, U u) {
            return CompletableFuture.supplyAsync(() -> {
                incCounter();
                return biFunctionEx.apply(t, u);
            });
        }
    }

    private static class AsyncMappingTriFunctionProvidingMetrics extends AbstractMapping
            implements TriFunction<Long, Long, Long, CompletableFuture<Long>> {
        private final TriFunction<Long, Long, Long, Long> triFunction;

        AsyncMappingTriFunctionProvidingMetrics(TriFunction<Long, Long, Long, Long> triFunction) {
            this.triFunction = triFunction;
        }

        @Override
        public CompletableFuture<Long> applyEx(Long t0, Long t1, Long t2) {
            return CompletableFuture.supplyAsync(() -> {
                incCounter();
                return triFunction.apply(t0, t1, t2);
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

    private static class EvictTriFunctionProvidingMetrics<T, U, V, R> extends AbstractEviction
            implements TriFunction<T, U, V, R> {

        private final TriFunction<T, U, V, R> evictFunction;

        EvictTriFunctionProvidingMetrics(TriFunction<T, U, V, R> evictFunction) {
            this.evictFunction = evictFunction;
        }

        @Override
        public R applyEx(T t, U u, V v) {
            incCounter();
            return evictFunction.apply(t, u, v);
        }
    }

}
