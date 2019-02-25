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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import org.junit.Test;

import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class WindowGroupAggregateTest extends PipelineStreamTestSupport {

    private static final Function<TimestampedEntry<String, Long>, Tuple2<Long, String>> TS_ENTRY_DISTINCT_FN =
            tse -> tuple2(tse.getTimestamp(), tse.getKey());

    private static final Function<TimestampedEntry<String, Long>, String> TS_ENTRY_FORMAT_FN =
            tse -> String.format("(%04d %s: %04d)", tse.getTimestamp(), tse.getKey(), tse.getValue());

    private static final Function<TimestampedEntry<String, Tuple2<Long, Long>>, String> TS_ENTRY_FORMAT_FN_2 =
            tse -> String.format("(%04d %s: %04d, %04d)",
                    tse.getTimestamp(), tse.getKey(), tse.getValue().f0(), tse.getValue().f1());

    private static final Function<TimestampedEntry<String, Tuple3<Long, Long, Long>>, String> TS_ENTRY_FORMAT_FN_3 =
            tse -> String.format("(%04d %s: %04d, %04d, %04d)",
                    tse.getTimestamp(), tse.getKey(), tse.getValue().f0(), tse.getValue().f1(), tse.getValue().f2());

    private static final Function<Entry<Long, Long>, String> MOCK_FORMAT_FN =
            e -> String.format("(%04d a: %04d)\n(%04d b: %04d)", e.getKey(), e.getValue(), e.getKey(), e.getValue());

    private static final Function<Entry<Long, Long>, String> MOCK_FORMAT_FN_2 =
            e -> String.format("(%04d a: %04d, %04d)\n(%04d b: %04d, %04d)",
                    e.getKey(), e.getValue(), e.getValue(),
                    e.getKey(), e.getValue(), e.getValue());

    private static final Function<Entry<Long, Long>, String> MOCK_FORMAT_FN_3 =
            e -> String.format("(%04d a: %04d, %04d, %04d)\n(%04d b: %04d, %04d, %04d)",
                    e.getKey(), e.getValue(), e.getValue(), e.getValue(),
                    e.getKey(), e.getValue(), e.getValue(), e.getValue());

    private static final AggregateOperation1<Entry<String, Integer>, LongAccumulator, Long> SUMMING =
            summingLong(Entry::getValue);

    @Test
    public void windowDefinition() {
        SlidingWindowDefinition tumbling = tumbling(2);
        StageWithKeyAndWindow<Integer, Integer> stage =
                sourceStageFromList(emptyList()).groupingKey(wholeItem()).window(tumbling);
        assertEquals(tumbling, stage.windowDefinition());
    }

    private class WindowTestFixture {
        final boolean emittingEarlyResults;

        final SlidingWindowDefinition tumblingWinDef = tumbling(4);

        List<Integer> input = sequence(itemCount);

        final String expectedString2 = new SlidingWindowSimulator(tumblingWinDef)
                .acceptStream(input.stream())
                .stringResults(MOCK_FORMAT_FN_2);

        final String expectedString3 = new SlidingWindowSimulator(tumblingWinDef)
                .acceptStream(input.stream())
                .stringResults(MOCK_FORMAT_FN_3);

        WindowTestFixture(boolean emittingEarlyResults) {
            this.emittingEarlyResults = emittingEarlyResults;
        }

        StreamStageWithKey<Entry<String, Integer>, String> newSourceStage() {
            return sourceStageFromList(input, emittingEarlyResults ? EARLY_RESULTS_PERIOD : 0)
                    .flatMap(i -> traverseItems(entry("a", i), entry("b", i)))
                    .groupingKey(Entry::getKey);
        }
    }

    @Test
    public void distinct() {
        // Given
        itemCount = (int) roundUp(itemCount, 2);
        int winSize = itemCount / 2;
        List<Integer> timestamps = sequence(itemCount);
        StageWithKeyAndWindow<Integer, Integer> windowed = sourceStageFromList(timestamps)
                .groupingKey(i -> i / 2)
                .window(tumbling(winSize));

        // When
        StreamStage<TimestampedItem<Integer>> distinct = windowed.distinct();

        // Then
        distinct.drainTo(sink);
        String expectedString = IntStream
                .range(0, itemCount)
                .mapToObj(i -> String.format("(%04d, %04d)", roundUp(i + 1, winSize), i / 2))
                .distinct()
                .sorted()
                .collect(joining("\n"));
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(
                        this.<Integer>sinkStreamOfTsItem(),
                        tsItem -> String.format("(%04d, %04d)", tsItem.timestamp(), tsItem.item() / 2))
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void distinct_withOutputFn() {
        // Given
        itemCount = (int) roundUp(itemCount, 2);
        int winSize = itemCount / 2;
        List<Integer> timestamps = sequence(itemCount);

        StageWithKeyAndWindow<Integer, Integer> windowed = sourceStageFromList(timestamps)
                .groupingKey(i -> i / 2)
                .window(tumbling(winSize));

        // When
        StreamStage<String> distinct = windowed.distinct(
                (start, end, item) -> String.format("(%04d, %04d)", end, item / 2)
        );

        // Then
        distinct.drainTo(sink);
        jet().newJob(p);
        String expectedString = IntStream
                .range(0, itemCount)
                .mapToObj(i -> String.format("(%04d, %04d)", roundUp(i + 1, winSize), i / 2))
                .distinct()
                .sorted()
                .collect(joining("\n"));
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkList.stream().map(String.class::cast), identity())
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void tumblingWindow() {
        testTumblingWindow(0L);
    }

    @Test
    public void tumblingWindow_withEarlyResults() {
        testTumblingWindow(EARLY_RESULTS_PERIOD);
    }

    private void testTumblingWindow(long earlyResultsPeriod) {
        // Given
        final int winSize = 4;
        WindowTestFixture fx = new WindowTestFixture(earlyResultsPeriod != 0);

        // When
        SlidingWindowDefinition wDef = tumbling(winSize).setEarlyResultsPeriod(earlyResultsPeriod);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .drainTo(sink);
        jet().newJob(p);

        String expectedString = new SlidingWindowSimulator(wDef)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);

        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void slidingWindow() {
        testSlidingWindow(0L);
    }

    @Test
    public void slidingWindow_withEarlyResults() {
        testSlidingWindow(EARLY_RESULTS_PERIOD);
    }

    private void testSlidingWindow(long earlyResultsPeriod) {
        // Given
        final int winSize = 4;
        final int slideBy = 2;
        WindowTestFixture fx = new WindowTestFixture(earlyResultsPeriod != 0);

        // When
        SlidingWindowDefinition wDef =
                (SlidingWindowDefinition) sliding(winSize, slideBy).setEarlyResultsPeriod(earlyResultsPeriod);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING)
                .drainTo(sink);
        jet().newJob(p);

        String expectedString = new SlidingWindowSimulator(wDef)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void slidingWindow_cascadingAggregations() {
        // Given
        List<Integer> input = asList(0, 1, 2);
        StreamStage<Entry<String, String>> srcStage =
                sourceStageFromList(input)
                        .flatMap(i -> traverseItems(entry("a", "a" + i), entry("b", "b" + i)));

        // When
        srcStage.groupingKey(Entry::getKey)
                .window(sliding(2, 1))
                .aggregate(mapping(Entry::getValue, AggregateOperations.toList()))
                .map(TimestampedEntry::getValue)
                .window(tumbling(1))
                .aggregate(AggregateOperations.toList(),
                        (start, end, listOfList) -> {
                            listOfList.sort(comparing(l -> l.get(0)));
                            return formatTsItem(end, listOfList.get(0), listOfList.get(1));
                        })
                .drainTo(sink);

        // Then
        jet().newJob(p);
        String expectedString = String.join("\n",
                formatTsItem(1, singletonList("a0"), singletonList("b0")),
                formatTsItem(2, asList("a0", "a1"), asList("b0", "b1")),
                formatTsItem(3, asList("a1", "a2"), asList("b1", "b2")),
                formatTsItem(4, singletonList("a2"), singletonList("b2"))
        );
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkList.stream().map(String.class::cast), identity())),
                ASSERT_TIMEOUT_SECONDS);
    }

    private static String formatTsItem(long timestamp, List<String> l1, List<String> l2) {
        return String.format("%04d: [%s, %s]", timestamp, l1, l2);
    }

    @Test
    public void sessionWindow() {
        // Given
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        WindowTestFixture fx = new WindowTestFixture(false);
        fx.input = fx.input
                .stream()
                .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                .collect(toList());

        // When
        SessionWindowDefinition wDef = session(sessionTimeout).setEarlyResultsPeriod(0L);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING,
                (start, end, key, sum) -> new TimestampedEntry<>(start, key, sum))
                .drainTo(sink);
        jet().newJob(p);

        String expectedString = new SessionWindowSimulator(wDef, sessionLength + sessionTimeout)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void sessionWindow_withEarlyResults() {
        // Given
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        WindowTestFixture fx = new WindowTestFixture(true);
        fx.input = fx.input
                .stream()
                .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                .collect(toList());

        // When
        SessionWindowDefinition wDef = session(sessionTimeout).setEarlyResultsPeriod(EARLY_RESULTS_PERIOD);
        StageWithKeyAndWindow<Entry<String, Integer>, String> windowed = fx.newSourceStage().window(wDef);

        // Then
        windowed.aggregate(SUMMING,
                (start, end, key, sum) ->
                        // suppress incomplete windows to get predictable results
                        end - start != sessionLength + sessionTimeout - 1
                                ? null
                                : new TimestampedEntry<>(start, key, sum))
                .drainTo(sink);
        jet().newJob(p);

        String expectedString = new SessionWindowSimulator(wDef, sessionLength + sessionTimeout)
                .acceptStream(fx.input.stream())
                .stringResults(MOCK_FORMAT_FN);
        assertTrueEventually(() -> assertEquals(
                expectedString,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN, TS_ENTRY_DISTINCT_FN)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate2_withSeparateAggrOps() {
        // Given
        final int winSize = 4;
        WindowTestFixture fx = new WindowTestFixture(false);
        SlidingWindowDefinition wDef = tumbling(winSize);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(wDef);

        // When
        StreamStage<TimestampedEntry<String, Tuple2<Long, Long>>> aggregated =
                stage0.aggregate2(SUMMING, fx.newSourceStage(), SUMMING);

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                fx.expectedString2,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN_2)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate2_withAggrOp2() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<TimestampedEntry<String, Tuple2<Long, Long>>> aggregated =
                stage0.aggregate2(fx.newSourceStage(), aggregateOperation2(SUMMING, SUMMING));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                fx.expectedString2,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN_2)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate2_withSeparateAggrOps_withOutputFn() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<String> aggregated = stage0.aggregate2(SUMMING,
                fx.newSourceStage(), SUMMING, (start, end, key, sum0, sum1) ->
                        TS_ENTRY_FORMAT_FN_2.apply(new TimestampedEntry<>(end, key, tuple2(sum0, sum1))));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        assertTrueEventually(() -> assertEquals(
                fx.expectedString2,
                streamToString(sinkList.stream().map(String.class::cast), identity())),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate2_withAggrOp2_withOutputFn() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<String> aggregated = stage0.aggregate2(
                fx.newSourceStage(), aggregateOperation2(SUMMING, SUMMING),
                (start, end, key, sums) -> TS_ENTRY_FORMAT_FN_2.apply(new TimestampedEntry<>(end, key, sums)));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        assertTrueEventually(() -> assertEquals(
                fx.expectedString2,
                streamToString(sinkList.stream().map(String.class::cast), identity())
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate3_withSeparateAggrOps() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<TimestampedEntry<String, Tuple3<Long, Long, Long>>> aggregated =
                stage0.aggregate3(SUMMING, fx.newSourceStage(), SUMMING, fx.newSourceStage(), SUMMING);

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                fx.expectedString3,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN_3)
        ), ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate3_withAggrOp3() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<TimestampedEntry<String, Tuple3<Long, Long, Long>>> aggregated =
                stage0.aggregate3(fx.newSourceStage(), fx.newSourceStage(),
                        aggregateOperation3(SUMMING, SUMMING, SUMMING));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                fx.expectedString3,
                streamToString(sinkStreamOfTsEntry(), TS_ENTRY_FORMAT_FN_3)),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate3_withSeparateAggrOps_withOutputFn() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<String> aggregated = stage0.aggregate3(SUMMING,
                fx.newSourceStage(), SUMMING,
                fx.newSourceStage(), SUMMING,
                (start, end, key, sum0, sum1, sum2) ->
                        TS_ENTRY_FORMAT_FN_3.apply(new TimestampedEntry<>(end, key, tuple3(sum0, sum1, sum2))));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        assertTrueEventually(() -> assertEquals(
                fx.expectedString3,
                streamToString(sinkList.stream().map(String.class::cast), identity())),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregate3_withAggrOp3_withOutputFn() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StageWithKeyAndWindow<Entry<String, Integer>, String> stage0 = fx.newSourceStage().window(fx.tumblingWinDef);

        // When
        StreamStage<String> aggregated = stage0.aggregate3(
                fx.newSourceStage(), fx.newSourceStage(),
                aggregateOperation3(SUMMING, SUMMING, SUMMING),
                (start, end, key, sums) -> TS_ENTRY_FORMAT_FN_3.apply(new TimestampedEntry<>(end, key, sums)));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        assertTrueEventually(() -> assertEquals(
                fx.expectedString3,
                streamToString(sinkList.stream().map(String.class::cast), identity())),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregateBuilder_withSeparateAggrOps() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);
        StreamStageWithKey<Entry<String, Integer>, String> stage0 = fx.newSourceStage();
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.newSourceStage();

        // When
        WindowGroupAggregateBuilder<String, Long> b = stage0.window(fx.tumblingWinDef).aggregateBuilder(SUMMING);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(stage1, SUMMING);
        StreamStage<String> aggregated = b.build((start, end, key, sums) ->
                String.format("(%04d %s: %04d, %04d)", end, key, sums.get(tag0), sums.get(tag1)));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        assertTrueEventually(() -> assertEquals(
                fx.expectedString2,
                streamToString(sinkList.stream().map(String.class::cast), identity())),
                ASSERT_TIMEOUT_SECONDS);
    }

    @Test
    public void aggregateBuilder_withComplexAggrOp() {
        // Given
        WindowTestFixture fx = new WindowTestFixture(false);

        // When
        StreamStageWithKey<Entry<String, Integer>, String> stage0 = fx.newSourceStage();
        StreamStageWithKey<Entry<String, Integer>, String> stage1 = fx.newSourceStage();

        WindowGroupAggregateBuilder1<Entry<String, Integer>, String> b = stage0
                .window(fx.tumblingWinDef)
                .aggregateBuilder();
        Tag<Entry<String, Integer>> tag0_in = b.tag0();
        Tag<Entry<String, Integer>> tag1_in = b.add(stage1);

        CoAggregateOperationBuilder b2 = coAggregateOperationBuilder();
        Tag<Long> tag0 = b2.add(tag0_in, SUMMING);
        Tag<Long> tag1 = b2.add(tag1_in, SUMMING);

        StreamStage<String> aggregated = b.build(
                b2.build(),
                (start, end, key, sums) ->
                        String.format("(%04d %s: %04d, %04d)", end, key, sums.get(tag0), sums.get(tag1)));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        assertTrueEventually(() -> assertEquals(
                fx.expectedString2,
                streamToString(sinkList.stream().map(String.class::cast), identity())),
                ASSERT_TIMEOUT_SECONDS);
    }

}
