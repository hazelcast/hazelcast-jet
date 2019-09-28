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
import com.hazelcast.jet.aggregate.AggregateOperation;
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
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
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

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private static final String FILTER_DROPPED = "dropped";
    private static final String FILTER_TOTAL = "total";

    private static final String MAPPED = "mapped";

    private static final String FLAT_MAPPED = "flat_mapped";

    private Pipeline pipeline;

    @Before
    public void before() {
        pipeline = Pipeline.create();
    }

    @Test
    public void filter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new FilteringPredicateProvidingMetrics<>(l -> l % 2 == 0))
                .drainTo(Sinks.logger());

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
    }

    @Test
    public void filterUsingContext_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterUsingContext(
                        ContextFactory.withCreateFn(i -> 0L),
                        new FilteringBiPredicateProvidingMetrics<>((ctx, l) -> l % 2 == ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
    }

    @Test
    public void filterUsingContextAsync_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filterUsingContextAsync(
                        ContextFactory.withCreateFn(i -> 0L),
                        new AsyncFilterBiFunctionProvidingMetrics<>((ctx, l) -> l % 2L == ctx)
                )
                .drainTo(Sinks.logger());

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
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

        assertCountersProduced(FILTER_DROPPED, 3, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 3, FILTER_TOTAL, 5);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
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

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4);
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
    public void fusedMapAndFilter_batch() {
        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(new FilteringPredicateProvidingMetrics<>(i -> i % 2 == 0))
                .map(new MappingFunctionProvidingMetrics<>(l -> l * 10))
                .drainTo(Sinks.logger());

        assertCountersProduced(FILTER_DROPPED, 2, MAPPED, 3, FILTER_TOTAL, 5);
    }

    @Test
    public void fusedMapAndFilter_stream() {
        pipeline.drawFrom(TestSources.items(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(summingLong((ToLongFunctionEx<Integer>) Integer::longValue))
                .map(WindowResult::result)
                .filter(new FilteringPredicateProvidingMetrics<>(i -> i % 2 == 0))
                .map(new MappingFunctionProvidingMetrics<>(l -> l * 10))
                .drainTo(assertAnyOrder(Arrays.asList(120L, 300L)));

        assertCountersProduced(FILTER_DROPPED, 2, FILTER_TOTAL, 4, MAPPED, 2);
    }

    @Test
    public void aggregate_1way_batch() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp =
                aggrOp("create", "add", "combine", "deduct", "export", "finish");

        pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .aggregate(aggrOp)
                .drainTo(assertOrdered(Collections.singletonList(15L)));

        assertCountersProduced("create", 6, "add", 5, "combine", 5, "deduct", 0, "export", 0, "finish", 1);
    }

    @Test
    public void aggregate_1way_batchWithKey() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp =
                aggrOp("create", "add", "combine", "deduct", "export", "finish");

        pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L))
                .groupingKey(l -> l % 3L)
                .aggregate(aggrOp)
                .drainTo(assertAnyOrder(Arrays.asList(entry(0L, 18L), entry(1L, 22L), entry(2L, 15L))));

        assertCountersProduced("create", 6, "add", 10, "combine", 3, "deduct", 0, "export", 0, "finish", 3);
    }

    @Test
    public void aggregate_2way_batch_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        BatchStage<Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L));
        BatchStage<Long> stage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L));
        stage1.aggregate2(op1, stage2, op2)
                .flatMap(t2 -> Traversers.traverseItems(t2.f0(), t2.f1()))
                .drainTo(assertOrdered(Arrays.asList(15L, 60L)));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 5, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 6, "add2", 3, "combine2", 5, "deduct2", 0, "export2", 0, "finish2", 1);
    }

    @Test
    public void aggregate_2way_batch_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        BatchStage<Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L));
        BatchStage<Long> stage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L));
        stage1.aggregate2(stage2, AggregateOperations.aggregateOperation2(op1, op2))
                .flatMap(t2 -> Traversers.traverseItems(t2.f0(), t2.f1()))
                .drainTo(assertOrdered(Arrays.asList(15L, 60L)));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 5, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 6, "add2", 3, "combine2", 5, "deduct2", 0, "export2", 0, "finish2", 1);
    }

    @Test
    public void aggregate_2way_batchWithKey_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        BatchStageWithKey<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(6L, 7L, 8L, 9L, 10L))
                .groupingKey(l -> l % 3L);
        stage1.aggregate2(op1, stage2, op2)
                .map(e -> entry(e.getKey(), e.getValue().f0() + e.getValue().f1()))
                .drainTo(assertAnyOrder(Arrays.asList(entry(0L, 18L), entry(1L, 22L), entry(2L, 15L))));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 3, "deduct1", 0, "export1", 0, "finish1", 3,
                "create2", 6, "add2", 5, "combine2", 3, "deduct2", 0, "export2", 0, "finish2", 3);
    }

    @Test
    public void aggregate_2way_batchWithKey_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        BatchStageWithKey<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(6L, 7L, 8L, 9L, 10L))
                .groupingKey(l -> l % 3L);
        stage1.aggregate2(stage2, AggregateOperations.aggregateOperation2(op1, op2))
                .map(e -> entry(e.getKey(), e.getValue().f0() + e.getValue().f1()))
                .drainTo(assertAnyOrder(Arrays.asList(entry(0L, 18L), entry(1L, 22L), entry(2L, 15L))));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 3, "deduct1", 0, "export1", 0, "finish1", 3,
                "create2", 6, "add2", 5, "combine2", 3, "deduct2", 0, "export2", 0, "finish2", 3);
    }

    @Test
    public void aggregate_3way_batch_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");
        AggregateOperation1<Long, LongAccumulator, Long> op3 =
                aggrOp("create3", "add3", "combine3", "deduct3", "export3", "finish3");

        BatchStage<Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L));
        BatchStage<Long> stage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L));
        BatchStage<Long> stage3 = pipeline.drawFrom(TestSources.items(100L));
        stage1.aggregate3(op1, stage2, op2, stage3, op3)
                .flatMap(t2 -> Traversers.traverseItems(t2.f0(), t2.f1()))
                .drainTo(assertOrdered(Arrays.asList(15L, 60L)));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 5, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 6, "add2", 3, "combine2", 5, "deduct2", 0, "export2", 0, "finish2", 1,
                "create3", 6, "add3", 1, "combine3", 5, "deduct3", 0, "export3", 0, "finish3", 1);
    }

    @Test
    public void aggregate_3way_batch_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");
        AggregateOperation1<Long, LongAccumulator, Long> op3 =
                aggrOp("create3", "add3", "combine3", "deduct3", "export3", "finish3");

        BatchStage<Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L));
        BatchStage<Long> stage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L));
        BatchStage<Long> stage3 = pipeline.drawFrom(TestSources.items(100L));
        stage1.aggregate3(stage2, stage3, AggregateOperations.aggregateOperation3(op1, op2, op3))
                .flatMap(t2 -> Traversers.traverseItems(t2.f0(), t2.f1()))
                .drainTo(assertOrdered(Arrays.asList(15L, 60L)));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 5, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 6, "add2", 3, "combine2", 5, "deduct2", 0, "export2", 0, "finish2", 1,
                "create3", 6, "add3", 1, "combine3", 5, "deduct3", 0, "export3", 0, "finish3", 1);
    }

    @Test
    public void aggregate_3way_batchWithKey_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");
        AggregateOperation1<Long, LongAccumulator, Long> op3 =
                aggrOp("create3", "add3", "combine3", "deduct3", "export3", "finish3");

        BatchStageWithKey<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(6L, 7L, 8L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage3 = pipeline.drawFrom(TestSources.items(9L, 10L))
                .groupingKey(l -> l % 3L);
        stage1.aggregate3(op1, stage2, op2, stage3, op3)
                .map(e -> entry(e.getKey(), e.getValue().f0() + e.getValue().f1() + e.getValue().f2()))
                .drainTo(assertAnyOrder(Arrays.asList(entry(0L, 18L), entry(1L, 22L), entry(2L, 15L))));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 3, "deduct1", 0, "export1", 0, "finish1", 3,
                "create2", 6, "add2", 3, "combine2", 3, "deduct2", 0, "export2", 0, "finish2", 3,
                "create3", 6, "add3", 2, "combine3", 3, "deduct3", 0, "export3", 0, "finish3", 3);
    }

    @Test
    public void aggregate_3way_batchWithKey_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");
        AggregateOperation1<Long, LongAccumulator, Long> op3 =
                aggrOp("create3", "add3", "combine3", "deduct3", "export3", "finish3");

        BatchStageWithKey<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(6L, 7L, 8L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage3 = pipeline.drawFrom(TestSources.items(9L, 10L))
                .groupingKey(l -> l % 3L);
        stage1.aggregate3(stage2, stage3, aggregateOperation3(op1, op2, op3))
                .map(e -> entry(e.getKey(), e.getValue().f0() + e.getValue().f1() + e.getValue().f2()))
                .drainTo(assertAnyOrder(Arrays.asList(entry(0L, 18L), entry(1L, 22L), entry(2L, 15L))));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 3, "deduct1", 0, "export1", 0, "finish1", 3,
                "create2", 6, "add2", 3, "combine2", 3, "deduct2", 0, "export2", 0, "finish2", 3,
                "create3", 6, "add3", 2, "combine3", 3, "deduct3", 0, "export3", 0, "finish3", 3);
    }

    private AggregateOperation1<Long, LongAccumulator, Long> aggrOp(String create, String add, String combine,
                                                                    String deduct, String export, String finish) {
        return AggregateOperation
                .withCreate(new AggrCreateFnProvidingMetrics(create))
                .andAccumulate(new AggrAccumulateFnProvidingMetrics(add))
                .andCombine(new AggrCombineFnProvidingMetrics(combine))
                .andDeduct(new AggrDeductFnProvidingMetrics(deduct))
                .andExport(new AggrResultFnProvidingMetrics(export))
                .andFinish(new AggrResultFnProvidingMetrics(finish));
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
            droppedCounter = context.registerCounter(FILTER_DROPPED);

            context.registerGauge(FILTER_TOTAL, totalCounter::get);
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

    private abstract static class AbstractMapping implements ProvidesMetrics, Serializable {

        private Counter mappedCounter;

        @Override
        public void registerMetrics(MetricsContext context) {
            if (mappedCounter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once!");
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

    private abstract static class AbstractFlatMapping implements ProvidesMetrics, Serializable {
        private Counter expandedCounter;

        @Override
        public void registerMetrics(MetricsContext context) {
            if (expandedCounter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once!");
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

    private abstract static class AbstractAggregateFunction implements ProvidesMetrics, Serializable {
        private final String metricName;

        private Counter counter;

        AbstractAggregateFunction(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public void registerMetrics(MetricsContext context) {
            if (counter != null) {
                /* We do this check explicitly because we want to detect situations when this method gets called
                multiple times on the same object, but with different context parameters. */
                throw new IllegalStateException("Should get initialised only once!");
            }
            counter = context.registerCounter(metricName);
        }

        void incrementCounter() {
            counter.increment();
        }
    }

    private static class AggrAccumulateFnProvidingMetrics extends AbstractAggregateFunction
            implements BiConsumerEx<LongAccumulator, Long>, ProvidesMetrics {
        AggrAccumulateFnProvidingMetrics(String metricName) {
            super(metricName);
        }

        @Override
        public void acceptEx(LongAccumulator longAccumulator, Long l) {
            longAccumulator.add(l);
            incrementCounter();
        }
    }

    private static class AggrCreateFnProvidingMetrics extends AbstractAggregateFunction
            implements SupplierEx<LongAccumulator> {
        AggrCreateFnProvidingMetrics(String metricName) {
            super(metricName);
        }

        @Override
        public LongAccumulator getEx() {
            incrementCounter();
            return new LongAccumulator();
        }
    }

    private static class AggrCombineFnProvidingMetrics extends AbstractAggregateFunction
            implements BiConsumerEx<LongAccumulator, LongAccumulator> {
        AggrCombineFnProvidingMetrics(String metricName) {
            super(metricName);
        }

        @Override
        public void acceptEx(LongAccumulator acc1, LongAccumulator acc2) {
            incrementCounter();
            acc1.add(acc2);
        }
    }

    private static class AggrDeductFnProvidingMetrics extends AbstractAggregateFunction
            implements BiConsumerEx<LongAccumulator, LongAccumulator> {
        AggrDeductFnProvidingMetrics(String metricName) {
            super(metricName);
        }

        @Override
        public void acceptEx(LongAccumulator acc1, LongAccumulator acc2) {
            incrementCounter();
            acc1.subtract(acc2);
        }
    }

    private static class AggrResultFnProvidingMetrics extends AbstractAggregateFunction
            implements FunctionEx<LongAccumulator, Long> {
        AggrResultFnProvidingMetrics(String metricName) {
            super(metricName);
        }

        @Override
        public Long applyEx(LongAccumulator acc) {
            incrementCounter();
            return acc.get();
        }
    }
}
