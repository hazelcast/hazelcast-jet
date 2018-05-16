/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class WindowAggregateTest extends PipelineStreamTestSupport {

    @Test
    public void when_setWindowDefinition_then_windowDefinitionReturnsIt() {
        Pipeline p = Pipeline.create();
        SlidingWindowDef tumbling = tumbling(2);
        StageWithWindow<Entry<Long, String>> stage =
                p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
                 .window(tumbling);
        assertEquals(tumbling, stage.windowDefinition());
    }

    @Test
    public void distinct() {
        // Given
        int winSize = itemCount / 2;
        List<Integer> timestamps = sequence(itemCount).stream()
                                                      .flatMap(i -> Stream.of(i, i))
                                                      .collect(toList());
        addToSrcMapJournal(timestamps);
        addToSrcMapJournal(closingItems);

        // For window size 2, streamInput looks like this (timestamp, item):
        // (0, 0), (0, 0), (1, 1), (1, 1), (2, 0), (2, 0), (3, 1), (3, 1), ...
        // I.e., there are duplicate items 0 and 1 in each window.
        StreamStage<Integer> streamInput = mapJournalSrcStage.addTimestamps(i -> i, 0)
                                                             .map(i -> i % winSize);

        // When
        StreamStage<TimestampedItem<Integer>> distinct = streamInput
                .window(tumbling(winSize))
                .distinct();

        // Then
        distinct.drainTo(sink);
        jet().newJob(p);
        // The expected output for window size 2 is (2, 0), (2, 1), (4, 0), (4, 1), ...
        Map<TimestampedItem<Integer>, Integer> expected = toBag(timestamps
                .stream()
                .map(i -> new TimestampedItem<>(winSize + i - i % winSize, i % winSize))
                .distinct()
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expected, sinkToBag()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void distinctBy_withOutputFn() {
        // Given
        int winSize = itemCount;
        DistributedFunction<Integer, Integer> keyFn = i -> i / 2;
        DistributedBiFunction<Long, Integer, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, keyFn.apply(item));
        List<Integer> timestamps = sequence(2 * itemCount);
        addToSrcMapJournal(timestamps);
        addToSrcMapJournal(closingItems);

        // When
        StreamStage<String> distinct = mapJournalSrcStage
                .addTimestamps(i -> i, 0)
                .window(tumbling(winSize))
                .distinctBy(keyFn, (start, end, item) -> formatFn.apply(end, item));

        // Then
        distinct.drainTo(sink);
        jet().newJob(p);
        // The expected output for window size 4 (timestamp, key):  (4, 0), (4, 1), (8, 2), (8, 3), ...
        Map<String, Integer> expectedKeys = toBag(timestamps
                .stream()
                .map(i -> formatFn.apply((long) winSize + i - i % winSize, i))
                .distinct()
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expectedKeys, sinkToBag()));
    }

    @Test
    public void tumblingWindow() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, item);

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        // When
        final int winSize = 4;
        StreamStage<String> aggregated = mapJournalSrcStage
                .addTimestamps(i -> i, 0)
                .window(tumbling(winSize))
                .aggregate(summingLong(i -> i), (start, end, sum) -> formatFn.apply(end, sum));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> start * start + winSize * (winSize - 1) / 2L;
        Map<String, Integer> expectedBag = toBag(input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> formatFn.apply((long) start + winSize, expectedWindowSum.apply(start)))
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void sessionWindow() {
        // Given
        final int sessionLength = 4;
        final int sessionTimeout = 2;
        List<Integer> input = sequence(itemCount).stream()
                                                 .map(ts -> ts + (ts / sessionLength) * sessionTimeout)
                                                 .collect(toList());
        DistributedBiFunction<Long, Long, String> formatFn =
                (timestamp, item) -> String.format("(%03d, %03d)", timestamp, item);

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        // When
        StreamStage<String> aggregated = mapJournalSrcStage
                .addTimestamps(i -> i, 0)
                .window(session(sessionTimeout))
                .aggregate(summingLong(i -> i), (start, end, sum) -> formatFn.apply(start, sum));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> sessionLength * (2 * start + sessionLength - 1) / 2L;
        Map<String, Integer> expectedBag = toBag(input
                .stream()
                .map(i -> i - i % (sessionLength + sessionTimeout))
                .distinct()
                .map(start -> formatFn.apply((long) start, expectedWindowSum.apply(start)))
                .collect(toList()));
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregate2() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Tuple2<Long, Long>, String> formatFn =
                (timestamp, sums) -> String.format("(%03d: %03d, %03d)", timestamp, sums.f0(), sums.f1());

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        addToMapJournal(srcMap1, input);
        addToMapJournal(srcMap1, closingItems);

        StreamStage<Integer> srcStage0 = mapJournalSrcStage.addTimestamps(i -> i, 0);
        StreamStage<Integer> srcStage1 = drawEventJournalValues(srcName1).addTimestamps(i -> i, 0);

        // When
        final int winSize = 4;
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        StreamStage<String> aggregated = srcStage0
                .window(tumbling(winSize))
                .aggregate2(
                        srcStage1,
                        aggregateOperation2(aggrOp, aggrOp, Tuple2::tuple2),
                        (start, end, sums) -> formatFn.apply(end, sums));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> start * start + winSize * (winSize - 1) / 2L;
        List<String> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return formatFn.apply((long) start + winSize, tuple2(sum, sum));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregate3() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Tuple3<Long, Long, Long>, String> formatFn =
                (timestamp, sums) -> String.format("(%03d: %03d, %03d, %03d)",
                        timestamp, sums.f0(), sums.f1(), sums.f2());

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName1 = journaledMapName();
        Map<String, Integer> srcMap1 = jet().getMap(srcName1);
        addToMapJournal(srcMap1, input);
        addToMapJournal(srcMap1, closingItems);

        String srcName2 = journaledMapName();
        Map<String, Integer> srcMap2 = jet().getMap(srcName2);
        addToMapJournal(srcMap2, input);
        addToMapJournal(srcMap2, closingItems);

        StreamStage<Integer> srcStage0 = mapJournalSrcStage.addTimestamps(i -> i, 0);
        StreamStage<Integer> srcStage1 = drawEventJournalValues(srcName1).addTimestamps(i -> i, 0);
        StreamStage<Integer> srcStage2 = drawEventJournalValues(srcName2).addTimestamps(i -> i, 0);

        // When
        final int winSize = 4;
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        StreamStage<String> aggregated = srcStage0
                .window(tumbling(winSize))
                .aggregate3(
                        srcStage1, srcStage2,
                        aggregateOperation3(aggrOp, aggrOp, aggrOp, Tuple3::tuple3),
                        (start, end, sums) -> formatFn.apply(end, sums));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> start * start + winSize * (winSize - 1) / 2L;
        List<String> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return formatFn.apply((long) start + winSize, tuple3(sum, sum, sum));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }

    @Test
    public void aggregateBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedBiFunction<Long, Tuple2<Long, Long>, String> formatFn =
                (timestamp, sums) -> String.format("(%03d: %03d, %03d)", timestamp, sums.f0(), sums.f1());

        addToSrcMapJournal(input);
        addToSrcMapJournal(closingItems);

        String srcName2 = journaledMapName();
        Map<String, Integer> srcMap2 = jet().getMap(srcName2);
        addToMapJournal(srcMap2, input);
        addToMapJournal(srcMap2, closingItems);

        StreamStage<Integer> srcStage1 = mapJournalSrcStage.addTimestamps(i -> i, 0);
        StreamStage<Integer> srcStage2 = drawEventJournalValues(srcName2).addTimestamps(i -> i, 0);

        // When
        final int winSize = 4;
        AggregateOperation1<Integer, LongAccumulator, Long> aggrOp = summingLong(i -> i);
        WindowAggregateBuilder<Integer> b = srcStage1.window(tumbling(winSize)).aggregateBuilder();
        b.add(srcStage2);
        StreamStage<String> aggregated = b.build(aggregateOperation2(aggrOp, aggrOp, Tuple2::tuple2),
                (start, end, sums) -> formatFn.apply(end, sums));

        // Then
        aggregated.drainTo(sink);
        jet().newJob(p);

        Function<Integer, Long> expectedWindowSum = start -> start * start + winSize * (winSize - 1) / 2L;
        List<String> expected = input
                .stream()
                .map(i -> i - i % winSize)
                .distinct()
                .map(start -> {
                    long sum = expectedWindowSum.apply(start);
                    return formatFn.apply((long) start + winSize, tuple2(sum, sum));
                })
                .collect(toList());
        Map<String, Integer> expectedBag = toBag(expected);
        assertTrueEventually(() -> assertEquals(expectedBag, sinkToBag()));
    }
}
