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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.AggregateBuilder;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.GroupAggregateBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserMetrics_AggregationTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private JetInstance instance;
    private Pipeline pipeline;

    @Before
    public void before() {
        instance = createJetMember();
        pipeline = Pipeline.create();
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
    public void aggregate_1way_streamWithSlidingWindow() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp =
                aggrOp("create", "add", "combine", "deduct", "export", "finish");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .aggregate(aggrOp)
                .setLocalParallelism(4)
                .map(WindowResult::result)
                .drainTo(assertAnyOrder(Arrays.asList(3L, 12L, 21L, 30L)));

        assertCountersProduced("create", 21, "add", 12, "combine", 12, "deduct", 0, "export", 0, "finish", 4);
    }

    @Test
    public void aggregate_1way_streamWithSessionWindow() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp =
                aggrOp("create", "add", "combine", "deduct", "export", "finish");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 16L, 17L, 18L, 19L, 20L, 21L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.session(5))
                .aggregate(aggrOp)
                .map(WindowResult::result)
                .drainTo(assertAnyOrder(Arrays.asList(15L, 111L)));

        assertCountersProduced("create", 2, "add", 12, "combine", 0, "deduct", 0, "export", 0, "finish", 2);
    }

    @Test
    public void aggregate_1way_streamWithSlidingWindowAndKey() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp =
                aggrOp("create", "add", "combine", "deduct", "export", "finish");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .groupingKey(l -> 0L)
                .aggregate(aggrOp)
                .setLocalParallelism(4)
                .map(WindowResult::result)
                .drainTo(assertAnyOrder(Arrays.asList(3L, 12L, 21L, 30L)));

        assertCountersProduced("create", 16, "add", 12, "combine", 4, "deduct", 0, "export", 0, "finish", 4);
    }

    @Test
    public void aggregate_1way_streamWithSessionWindowAndKey() {
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp =
                aggrOp("create", "add", "combine", "deduct", "export", "finish");

        pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 16L, 17L, 18L, 19L, 20L, 21L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.session(5))
                .groupingKey(l -> 0L)
                .aggregate(aggrOp)
                .map(WindowResult::result)
                .drainTo(assertAnyOrder(Arrays.asList(15L, 111L)));

        assertCountersProduced("create", 2, "add", 12, "combine", 0, "deduct", 0, "export", 0, "finish", 2);
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
    public void aggregate_2way_streamWithSlidingWindow_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithWindow<Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3));
        StreamStage<Long> stage2 = pipeline.drawFrom(TestSources.items(8L, 9L, 10L, 11L))
                .addTimestamps(i -> i, 0L);

        stage1.aggregate2(op1, stage2, op2)
                .setLocalParallelism(4)
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 21, "add1", 8, "combine1", 12, "deduct1", 0, "export1", 0, "finish1", 4,
                "create2", 21, "add2", 4, "combine2", 12, "deduct2", 0, "export2", 0, "finish2", 4
        );
    }

    @Test
    public void aggregate_2way_streamWithSlidingWindow_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithWindow<Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3));
        StreamStage<Long> stage2 = pipeline.drawFrom(TestSources.items(8L, 9L, 10L, 11L))
                .addTimestamps(i -> i, 0L);

        stage1.aggregate2(stage2, AggregateOperations.aggregateOperation2(op1, op2))
                .setLocalParallelism(4)
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 21, "add1", 8, "combine1", 12, "deduct1", 0, "export1", 0, "finish1", 4,
                "create2", 21, "add2", 4, "combine2", 12, "deduct2", 0, "export2", 0, "finish2", 4
        );
    }

    @Test
    public void aggregate_2way_streamWithSessionWindow_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithWindow<Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 10L, 11L, 12L, 13L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.session(5));
        StreamStage<Long> stage2 = pipeline.drawFrom(TestSources.items(4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L);

        stage1.aggregate2(op1, stage2, op2)
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 1, "add1", 8, "combine1", 0, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 1, "add2", 4, "combine2", 0, "deduct2", 0, "export2", 0, "finish2", 1
        );
    }

    @Test
    public void aggregate_2way_streamWithSessionWindow_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithWindow<Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 10L, 11L, 12L, 13L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.session(5));
        StreamStage<Long> stage2 = pipeline.drawFrom(TestSources.items(4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L);

        stage1.aggregate2(stage2, AggregateOperations.aggregateOperation2(op1, op2))
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 1, "add1", 8, "combine1", 0, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 1, "add2", 4, "combine2", 0, "deduct2", 0, "export2", 0, "finish2", 1
        );
    }

    @Test
    public void aggregate_2way_streamWithSlidingWindowAndKey_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithKeyAndWindow<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .groupingKey(l -> 0L);
        StreamStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(8L, 9L, 10L, 11L))
                .addTimestamps(i -> i, 0L)
                .groupingKey(l -> 0L);

        stage1.aggregate2(op1, stage2, op2)
                .setLocalParallelism(4)
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 16, "add1", 8, "combine1", 4, "deduct1", 0, "export1", 0, "finish1", 4,
                "create2", 16, "add2", 4, "combine2", 4, "deduct2", 0, "export2", 0, "finish2", 4
        );
    }

    @Test
    public void aggregate_2way_streamWithSlidingWindowAndKey_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithKeyAndWindow<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.tumbling(3))
                .groupingKey(l -> 0L);
        StreamStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(8L, 9L, 10L, 11L))
                .addTimestamps(i -> i, 0L)
                .groupingKey(l -> 0L);

        stage1.aggregate2(stage2, AggregateOperations.aggregateOperation2(op1, op2))
                .setLocalParallelism(4)
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 16, "add1", 8, "combine1", 4, "deduct1", 0, "export1", 0, "finish1", 4,
                "create2", 16, "add2", 4, "combine2", 4, "deduct2", 0, "export2", 0, "finish2", 4
        );
    }

    @Test
    public void aggregate_2way_streamWithSessionWindowAndKey_simple() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithKeyAndWindow<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 10L, 11L, 12L, 13L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.session(5))
                .groupingKey(l -> 0L);
        StreamStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L)
                .groupingKey(l -> 0L);

        stage1.aggregate2(op1, stage2, op2)
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 1, "add1", 8, "combine1", 0, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 1, "add2", 4, "combine2", 0, "deduct2", 0, "export2", 0, "finish2", 1
        );
    }

    @Test
    public void aggregate_2way_streamWithSessionWindowAndKey_merged() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");

        StageWithKeyAndWindow<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(0L, 1L, 2L, 3L, 10L, 11L, 12L, 13L))
                .addTimestamps(i -> i, 0L)
                .window(WindowDefinition.session(5))
                .groupingKey(l -> 0L);
        StreamStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(4L, 5L, 6L, 7L))
                .addTimestamps(i -> i, 0L)
                .groupingKey(l -> 0L);

        stage1.aggregate2(stage2, AggregateOperations.aggregateOperation2(op1, op2))
                .drainTo(Sinks.logger());

        assertCountersProduced(
                "create1", 1, "add1", 8, "combine1", 0, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 1, "add2", 4, "combine2", 0, "deduct2", 0, "export2", 0, "finish2", 1
        );
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

    @Test
    public void aggregate_3way_streamWithSlidingWindow_simple() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSlidingWindow_merged() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSessionWindow_simple() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSessionWindow_merged() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSlidingWindowAndKey_simple() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSlidingWindowAndKey_merged() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSessionWindowAndKey_simple() {
        //todo
    }

    @Test
    public void aggregate_3way_streamWithSessionWindowAndKey_merged() {
        //todo
    }

    @Test
    public void aggregate_builder_batch() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");
        AggregateOperation1<Long, LongAccumulator, Long> op3 =
                aggrOp("create3", "add3", "combine3", "deduct3", "export3", "finish3");
        AggregateOperation1<Long, LongAccumulator, Long> op4 =
                aggrOp("create4", "add4", "combine4", "deduct4", "export4", "finish4");

        BatchStage<Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L));
        BatchStage<Long> stage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L));
        BatchStage<Long> stage3 = pipeline.drawFrom(TestSources.items(100L, 200L));
        BatchStage<Long> stage4 = pipeline.drawFrom(TestSources.items(1000L));

        AggregateBuilder<Long> builder = stage1.aggregateBuilder(op1);
        Tag<Long> tag0 = builder.tag0();
        Tag<Long> tag1 = builder.add(stage2, op2);
        Tag<Long> tag2 = builder.add(stage3, op3);
        Tag<Long> tag3 = builder.add(stage4, op4);
        builder.build()
                .flatMap(ibt -> Traversers.traverseItems(ibt.get(tag0), ibt.get(tag1), ibt.get(tag2), ibt.get(tag3)))
                .drainTo(assertOrdered(Arrays.asList(15L, 60L, 300L, 1000L)));

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 5, "deduct1", 0, "export1", 0, "finish1", 1,
                "create2", 6, "add2", 3, "combine2", 5, "deduct2", 0, "export2", 0, "finish2", 1,
                "create3", 6, "add3", 2, "combine3", 5, "deduct3", 0, "export3", 0, "finish3", 1,
                "create4", 6, "add4", 1, "combine4", 5, "deduct4", 0, "export4", 0, "finish4", 1);
    }

    @Test
    public void aggregate_builder_batchWithKey() {
        AggregateOperation1<Long, LongAccumulator, Long> op1 =
                aggrOp("create1", "add1", "combine1", "deduct1", "export1", "finish1");
        AggregateOperation1<Long, LongAccumulator, Long> op2 =
                aggrOp("create2", "add2", "combine2", "deduct2", "export2", "finish2");
        AggregateOperation1<Long, LongAccumulator, Long> op3 =
                aggrOp("create3", "add3", "combine3", "deduct3", "export3", "finish3");
        AggregateOperation1<Long, LongAccumulator, Long> op4 =
                aggrOp("create4", "add4", "combine4", "deduct4", "export4", "finish4");

        BatchStageWithKey<Long, Long> stage1 = pipeline.drawFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage2 = pipeline.drawFrom(TestSources.items(10L, 20L, 30L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage3 = pipeline.drawFrom(TestSources.items(100L, 200L))
                .groupingKey(l -> l % 3L);
        BatchStageWithKey<Long, Long> stage4 = pipeline.drawFrom(TestSources.items(1000L))
                .groupingKey(l -> l % 3L);

        GroupAggregateBuilder<Long, Long> builder = stage1.aggregateBuilder(op1);
        builder.tag0();
        builder.add(stage2, op2);
        builder.add(stage3, op3);
        builder.add(stage4, op4);
        builder.build().drainTo(Sinks.logger());

        assertCountersProduced("create1", 6, "add1", 5, "combine1", 3, "deduct1", 0, "export1", 0, "finish1", 3,
                "create2", 6, "add2", 3, "combine2", 3, "deduct2", 0, "export2", 0, "finish2", 3,
                "create3", 6, "add3", 2, "combine3", 3, "deduct3", 0, "export3", 0, "finish3", 3,
                "create4", 6, "add4", 1, "combine4", 3, "deduct4", 0, "export4", 0, "finish4", 3);
    }

    @Test
    public void aggregate_builder_streamWithSlidingWindow() {
        //todo
    }

    @Test
    public void aggregate_builder_streamWithSessionWindow() {
        //todo
    }

    @Test
    public void aggregate_builder_streamWithSlidingWindowAndKey() {
        //todo
    }

    @Test
    public void aggregate_builder_streamWithSessionWindowAndKey() {
        //todo
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
