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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.toBagsByTag;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.aggregate.AggregateOperations.toThreeBags;
import static com.hazelcast.jet.aggregate.AggregateOperations.toTwoBags;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BatchAggregateTest extends PipelineTestSupport {

    private BatchStage<Integer> srcStage;

    @Before
    public void before() {
        srcStage = p.drawFrom(mapValuesSource(srcName));
    }

    @Test
    public void aggregate() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchStage<Set<Integer>> aggregated = srcStage.aggregate(toSet());

        //Then
        aggregated.drainTo(sink);
        execute();
        assertEquals(toBag(singletonList(new HashSet<>(input))), sinkToBag());
    }

    @Test
    public void aggregate2() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn = i -> i + "-x";
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        BatchStage<String> stage1 = p.drawFrom(mapValuesSource(src1Name)).map(mapFn);

        // When
        BatchStage<TwoBags<Integer, String>> aggregated = srcStage.aggregate2(stage1, toTwoBags());

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        @SuppressWarnings("unchecked")
        TwoBags<Integer, String> actual = (TwoBags<Integer, String>) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(toBag(input), toBag(actual.bag0()));
        assertEquals(toBag(input.stream().map(mapFn).collect(toList())), toBag(actual.bag1()));
    }

    @Test
    public void aggregate3() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn1 = i -> i + "-a";
        DistributedFunction<Integer, String> mapFn2 = i -> i + "-b";
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<String> stage1 = p.drawFrom(mapValuesSource(src1Name)).map(mapFn1);
        BatchStage<String> stage2 = p.drawFrom(mapValuesSource(src2Name)).map(mapFn2);

        // When
        BatchStage<ThreeBags<Integer, String, String>> aggregated =
                srcStage.aggregate3(stage1, stage2, toThreeBags());

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        @SuppressWarnings("unchecked")
        ThreeBags<Integer, String, String> actual = (ThreeBags<Integer, String, String>) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(toBag(input), toBag(actual.bag0()));
        assertEquals(toBag(input.stream().map(mapFn1).collect(toList())), toBag(actual.bag1()));
        assertEquals(toBag(input.stream().map(mapFn2).collect(toList())), toBag(actual.bag2()));
    }

    @Test
    public void aggregateBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn1 = i -> i + "-a";
        DistributedFunction<Integer, String> mapFn2 = i -> i + "-b";
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<String> stage1 = p.drawFrom(mapValuesSource(src1Name)).map(mapFn1);
        BatchStage<String> stage2 = p.drawFrom(mapValuesSource(src2Name)).map(mapFn2);

        // When
        AggregateBuilder<Integer> b = srcStage.aggregateBuilder();
        Tag<Integer> tag0 = b.tag0();
        Tag<String> tag1 = b.add(stage1);
        Tag<String> tag2 = b.add(stage2);
        BatchStage<BagsByTag> aggregated = b.build(toBagsByTag(tag0, tag1, tag2));

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        BagsByTag actual = (BagsByTag) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(toBag(input), toBag(actual.bag(tag0)));
        assertEquals(toBag(input.stream().map(mapFn1).collect(toList())), toBag(actual.bag(tag1)));
        assertEquals(toBag(input.stream().map(mapFn2).collect(toList())), toBag(actual.bag(tag2)));
    }

    @Test
    public void groupAggregate() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i % 5;
        putToBatchSrcMap(input);

        // When
        BatchStage<Entry<Integer, Long>> aggregated = srcStage
                .groupingKey(keyFn)
                .aggregate(AggregateOperations.summingLong(i -> i));

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected = input.stream().collect(groupingBy(keyFn, Collectors.summingLong(i -> i)));
        assertEquals(toBag(expected.entrySet()), sinkToBag());
    }

    @Test
    public void groupAggregate2() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        AggregateOperation1<Integer, ?, Long> aggrOp = AggregateOperations.summingLong(i -> i);
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        String src1Name = randomMapName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        BatchStage<Entry<Integer, Tuple2<Long, Long>>> aggregated = stage0
                .aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp, Tuple2::tuple2));

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected0 = input.stream()
                                            .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expected1 = input.stream()
                                            .map(mapFn1)
                                            .collect(groupingBy(keyFn, collectOp));
        for (Object item : sinkList) {
            @SuppressWarnings("unchecked")
            Entry<Integer, Tuple2<Long, Long>> e = (Entry<Integer, Tuple2<Long, Long>>) item;
            Integer key = e.getKey();
            Tuple2<Long, Long> value = e.getValue();
            assertEquals(expected0.getOrDefault(key, 0L), value.f0());
            assertEquals(expected1.getOrDefault(key, 0L), value.f1());
        }
    }

    @Test
    public void groupAggregate2_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        AggregateOperation1<Integer, ?, Long> aggrOp = AggregateOperations.summingLong(i -> i);
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        long a = 37;
        DistributedBiFunction<Integer, Tuple2<Long, Long>, Long> outputFn = (k, v) -> v.f1() + a * (v.f0() + a * k);
        String src1Name = randomMapName();
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        BatchStage<Long> aggregated = stage0
                .aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp, Tuple2::tuple2), outputFn);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expectedAggr0 = input.stream()
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr1 = input.stream()
                                                .map(mapFn1)
                                                .collect(groupingBy(keyFn, collectOp));
        HashSet<Integer> keys = new HashSet<>(expectedAggr0.keySet());
        keys.addAll(expectedAggr1.keySet());
        List<Long> expectedOutput = keys
            .stream()
            .map(k -> outputFn.apply(k, tuple2(expectedAggr0.getOrDefault(k, 0L), expectedAggr1.getOrDefault(k, 0L))))
            .collect(toList());
        assertEquals(toBag(expectedOutput), sinkToBag());
    }

    @Test
    public void groupAggregate3() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        DistributedFunction<Integer, Integer> mapFn2 = i -> 100 * i;
        AggregateOperation1<Integer, ?, Long> aggrOp = AggregateOperations.summingLong(i -> i);
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        BatchStage<Integer> srcStage2 = p.drawFrom(mapValuesSource(src2Name))
                                         .map(mapFn2);
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage2 = srcStage2.groupingKey(keyFn);
        BatchStage<Entry<Integer, Tuple3<Long, Long, Long>>> aggregated = stage0
                .aggregate3(stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp, Tuple3::tuple3));

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected0 = input.stream()
                                            .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expected1 = input.stream()
                                            .map(mapFn1)
                                            .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expected2 = input.stream()
                                            .map(mapFn2)
                                            .collect(groupingBy(keyFn, collectOp));
        for (Object item : sinkList) {
            @SuppressWarnings("unchecked")
            Entry<Integer, Tuple3<Long, Long, Long>> e = (Entry<Integer, Tuple3<Long, Long, Long>>) item;
            Integer key = e.getKey();
            Tuple3<Long, Long, Long> value = e.getValue();
            assertEquals(expected0.getOrDefault(key, 0L), value.f0());
            assertEquals(expected1.getOrDefault(key, 0L), value.f1());
            assertEquals(expected2.getOrDefault(key, 0L), value.f2());
        }
    }

    @Test
    public void groupAggregate3_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        DistributedFunction<Integer, Integer> mapFn2 = i -> 100 * i;
        AggregateOperation1<Integer, ?, Long> aggrOp = AggregateOperations.summingLong(i -> i);
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        long a = 37;
        DistributedBiFunction<Integer, Tuple3<Long, Long, Long>, Long> outputFn = (k, v) ->
                v.f2() + a * (v.f1() + a * (v.f0() + a * k));
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        BatchStage<Integer> srcStage2 = p.drawFrom(mapValuesSource(src2Name))
                                         .map(mapFn2);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage2 = srcStage2.groupingKey(keyFn);
        BatchStage<Long> aggregated = stage0
                .aggregate3(stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp, Tuple3::tuple3), outputFn);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expectedAggr0 = input.stream()
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr1 = input.stream()
                                                .map(mapFn1)
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr2 = input.stream()
                                                .map(mapFn2)
                                                .collect(groupingBy(keyFn, collectOp));
        HashSet<Integer> keys = new HashSet<>(expectedAggr0.keySet());
        keys.addAll(expectedAggr1.keySet());
        keys.addAll(expectedAggr2.keySet());
        List<Long> expectedOutput = keys
            .stream()
            .map(k -> outputFn.apply(k, tuple3(
                    expectedAggr0.getOrDefault(k, 0L),
                    expectedAggr1.getOrDefault(k, 0L),
                    expectedAggr2.getOrDefault(k, 0L)
            )))
            .collect(toList());
        assertEquals(toBag(expectedOutput), sinkToBag());
    }

    @Test
    public void groupAggregateBuilder_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        DistributedFunction<Integer, Integer> mapFn2 = i -> 100 * i;
        AggregateOperation1<Integer, ?, Long> aggrOp = AggregateOperations.summingLong(i -> i);
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        BatchStage<Integer> srcStage2 = p.drawFrom(mapValuesSource(src2Name))
                                         .map(mapFn2);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        StageWithGrouping<Integer, Integer> stage2 = srcStage2.groupingKey(keyFn);
        GroupAggregateBuilder<Integer, Integer> b = stage0.aggregateBuilder();
        Tag<Integer> inTag0 = b.tag0();
        Tag<Integer> inTag1 = b.add(stage1);
        Tag<Integer> inTag2 = b.add(stage2);

        CoAggregateOperationBuilder b2 = coAggregateOperationBuilder();
        Tag<Long> outTag0 = b2.add(inTag0, aggrOp);
        Tag<Long> outTag1 = b2.add(inTag1, aggrOp);
        Tag<Long> outTag2 = b2.add(inTag2, aggrOp);
        AggregateOperation<Object[], ItemsByTag> aggrOpN = b2.build();

        long a = 37;
        @SuppressWarnings("ConstantConditions")
        DistributedBiFunction<Integer, ItemsByTag, Long> outputFn = (k, v) ->
                v.get(outTag2) + a * (v.get(outTag1) + a * (v.get(outTag0) + a * k));
        BatchStage<Long> aggregated = b.build(aggrOpN, outputFn);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expectedAggr0 = input.stream()
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr1 = input.stream()
                                                .map(mapFn1)
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr2 = input.stream()
                                                .map(mapFn2)
                                                .collect(groupingBy(keyFn, collectOp));
        HashSet<Integer> keys = new HashSet<>(expectedAggr0.keySet());
        keys.addAll(expectedAggr1.keySet());
        keys.addAll(expectedAggr2.keySet());
        List<Long> expectedOutput = keys
            .stream()
            .map(k -> outputFn.apply(k, itemsByTag(
                    outTag0, expectedAggr0.getOrDefault(k, 0L),
                    outTag1, expectedAggr1.getOrDefault(k, 0L),
                    outTag2, expectedAggr2.getOrDefault(k, 0L)
            )))
            .collect(toList());
        assertEquals(toBag(expectedOutput), sinkToBag());
    }
}
