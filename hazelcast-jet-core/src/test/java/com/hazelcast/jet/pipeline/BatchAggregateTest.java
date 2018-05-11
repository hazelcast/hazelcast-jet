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

import com.hazelcast.core.IList;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.aggregate.AggregateOperations.toThreeBags;
import static com.hazelcast.jet.aggregate.AggregateOperations.toTwoBags;
import static com.hazelcast.jet.datamodel.ThreeBags.threeBags;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        putToBatchSrcMap(input);
        String src2Name = randomMapName();
        putToMap(jet().getMap(src2Name), input);

        srcList.add(1);
        srcList.add(2);
        srcList.add(3);
        IList<String> srcList2 = jet().getList("srcList2");
        srcList2.add("a");
        srcList2.add("b");
        srcList2.add("c");

        // When
        BatchStage<Integer> stage1 = p.drawFrom(Sources.list("srcList2"));
        p.drawFrom(Sources.list("list"))
         .aggregate2(stage1, toTwoBags())
         .drainTo(Sinks.list("sink"));
        jet().newJob(p).join();

        //Then
        TwoBags twoBags = (TwoBags) jet().getHazelcastInstance().getList("sink").iterator().next();
        assertNotNull(twoBags);
        sort(twoBags);
        assertEquals(TwoBags.twoBags(srcList, srcList2), twoBags);
    }

    @Test
    public void aggregate3() {
        // Given
        IListJet<Integer> list = jet().getList("list");
        list.add(1);
        list.add(2);
        list.add(3);
        IListJet<String> list1 = jet().getList("list1");
        list1.add("a");
        list1.add("b");
        list1.add("c");
        IListJet<Double> list2 = jet().getList("list2");
        list2.add(6.0d);
        list2.add(7.0d);
        list2.add(8.0d);

        // When
        Pipeline p = Pipeline.create();
        BatchStage<String> stage1 = p.drawFrom(Sources.list("list1"));
        BatchStage<Double> stage2 = p.drawFrom(Sources.list("list2"));
        p.drawFrom(Sources.list("list"))
         .aggregate3(stage1, stage2, toThreeBags())
         .drainTo(Sinks.list("sink"));
        jet().newJob(p).join();

        //Then
        ThreeBags threeBags = (ThreeBags) jet().getHazelcastInstance().getList("sink").iterator().next();
        assertNotNull(threeBags);
        sort(threeBags);
        assertEquals(threeBags(list, list1, list2), threeBags);
    }


    @Test
    public void aggregateBuilder() {
        // Given
        IListJet<Integer> list = jet().getList("list");
        list.add(1);
        list.add(2);
        list.add(3);
        IListJet<String> list1 = jet().getList("list1");
        list1.add("a");
        list1.add("b");
        list1.add("c");
        IListJet<Double> list2 = jet().getList("list2");
        list2.add(6.0d);
        list2.add(7.0d);
        list2.add(8.0d);

        // When
        Pipeline p = Pipeline.create();
        BatchStage<Integer> stage0 = p.drawFrom(Sources.list("list"));
        BatchStage<String> stage1 = p.drawFrom(Sources.list("list1"));
        BatchStage<Double> stage2 = p.drawFrom(Sources.list("list2"));


        AggregateBuilder<Integer> builder = stage0.aggregateBuilder();
        Tag<Integer> tag0 = builder.tag0();
        Tag<String> tag1 = builder.add(stage1);
        Tag<Double> tag2 = builder.add(stage2);

        BatchStage<ThreeBags> resultStage = builder.build(AggregateOperation
                .withCreate(ThreeBags::threeBags)
                .andAccumulate(tag0, (acc, item0) -> acc.bag0().add(item0))
                .andAccumulate(tag1, (acc, item1) -> acc.bag1().add(item1))
                .andAccumulate(tag2, (acc, item2) -> acc.bag2().add(item2))
                .andCombine(ThreeBags::combineWith)
                .andDeduct(ThreeBags::deduct)
                .andFinish(ThreeBags::finish));

        resultStage.drainTo(Sinks.list("sink"));
        jet().newJob(p).join();

        //Then
        ThreeBags threeBags = (ThreeBags) jet().getHazelcastInstance().getList("sink").iterator().next();
        assertNotNull(threeBags);
        sort(threeBags);
        assertEquals(threeBags(list, list1, list2), threeBags);
    }

    @Test
    public void groupAggregate2() {
        //Given
        String src1Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name));
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(wholeItem());
        BatchStage<Entry<Integer, Long>> coGrouped = stage0.aggregate2(stage1,
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 11L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void groupAggregate2_withOutputFn() {
        //Given
        String src1Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name));
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = srcStage1.groupingKey(wholeItem());
        BatchStage<Long> coGrouped = stage0.aggregate2(stage1,
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get), (k, v) -> v);
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Long> expected = IntStream.range(1, 100)
                                       .mapToObj(i -> 11L * i)
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void groupAggregate3() {
        //Given
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToBatchSrcMap(input);
        String src1Name = HazelcastTestSupport.randomName();
        String src2Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> src1 = p.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> src2 = p.drawFrom(mapValuesSource(src2Name));
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = src1.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage2 = src2.groupingKey(wholeItem());
        BatchStage<Entry<Integer, Long>> coGrouped = stage0.aggregate3(stage1, stage2,
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andAccumulate2((count, item) -> count.add(100))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 111L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void groupAggregate3_withOutputFn() {
        //Given
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToBatchSrcMap(input);
        String src1Name = HazelcastTestSupport.randomName();
        String src2Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> src1 = p.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> src2 = p.drawFrom(mapValuesSource(src2Name));
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = src1.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage2 = src2.groupingKey(wholeItem());
        BatchStage<Long> coGrouped = stage0.aggregate3(stage1, stage2,
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andAccumulate2((count, item) -> count.add(100))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get),
                (k, v) -> v);
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Long> expected = IntStream.range(1, 100)
                                       .mapToObj(i ->111L * i)
                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void coGroupBuilder() {
        //Given
        List<Integer> input = IntStream.range(1, 100).boxed()
                                       .flatMap(i -> Collections.nCopies(i, i).stream())
                                       .collect(toList());
        putToBatchSrcMap(input);
        String src1Name = HazelcastTestSupport.randomName();
        String src2Name = HazelcastTestSupport.randomName();
        BatchStage<Integer> src1 = p.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> src2 = p.drawFrom(mapValuesSource(src2Name));
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        StageWithGrouping<Integer, Integer> stage0 = srcStage.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage1 = src1.groupingKey(wholeItem());
        StageWithGrouping<Integer, Integer> stage2 = src2.groupingKey(wholeItem());
        GroupAggregateBuilder<Integer, Integer> b = stage0.aggregateBuilder();
        Tag<Integer> tag0 = b.tag0();
        Tag<Integer> tag1 = b.add(stage1);
        Tag<Integer> tag2 = b.add(stage2);
        BatchStage<Entry<Integer, Long>> coGrouped = b.build(AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0, (count, item) -> count.add(1))
                .andAccumulate(tag1, (count, item) -> count.add(10))
                .andAccumulate(tag2, (count, item) -> count.add(100))
                .andCombine(LongAccumulator::add)
                .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);
        execute();

        // Then
        List<Entry<Integer, Long>> expected = IntStream.range(1, 100)
                                                       .mapToObj(i -> entry(i, 111L * i))
                                                       .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    public void sort(ThreeBags threeBags) {
        Collections.sort((List) threeBags.bag0());
        Collections.sort((List) threeBags.bag1());
        Collections.sort((List) threeBags.bag2());
    }

    public void sort(TwoBags twoBags) {
        Collections.sort((List) twoBags.bag0());
        Collections.sort((List) twoBags.bag1());
    }
}
