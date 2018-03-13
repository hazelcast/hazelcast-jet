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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SlidingWindowDef;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StreamStageWithGrouping;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.WindowGroupAggregateBuilder;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import java.util.HashSet;
import java.util.Map.Entry;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.aggregate.AggregateOperations.toThreeBags;
import static com.hazelcast.jet.aggregate.AggregateOperations.toTwoBags;
import static com.hazelcast.jet.core.TestUtil.set;
import static com.hazelcast.jet.core.test.TestSupport.listToString;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowGroupTransform_IntegrationTest extends JetTestSupport {

    private JetInstance instance;

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(
                new EventJournalConfig().setMapName("source*").setEnabled(true));
        config.getHazelcastConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        instance = createJetMember(config);
    }

    @Test
    public void testWindowDefinition() {
        Pipeline p = Pipeline.create();
        SlidingWindowDef tumbling = WindowDefinition.tumbling(2);
        StageWithGroupingAndWindow<Entry<Long, String>, Character> stage =
                p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
                 .groupingKey(entry -> entry.getValue().charAt(0))
                 .window(tumbling);
        assertEquals(tumbling, stage.windowDefinition());
    }

    @Test
    public void testSliding_groupingFirst() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .groupingKey(entry -> entry.getValue().charAt(0))
         .window(WindowDefinition.tumbling(2))
         .aggregate(toSet())
         .drainTo(Sinks.list("sink"));

        testSliding(p);
    }

    @Test
    public void testSliding_windowFirst() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.tumbling(2))
         .groupingKey(entry -> entry.getValue().charAt(0))
         .aggregate(toSet())
         .drainTo(Sinks.list("sink"));

        testSliding(p);
    }

    private void testSliding(Pipeline p) {
        IMap<Long, String> map = instance.getMap("source");
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");
        map.put(3L, "booze");
        map.put(10L, "flush-item");

        instance.newJob(p);

        assertTrueEventually(() -> {
            assertEquals(
                    set(
                            new TimestampedEntry<>(2, 'f', set(entry(0L, "foo"))),
                            new TimestampedEntry<>(2, 'b', set(entry(1L, "bar"))),
                            new TimestampedEntry<>(4, 'b', set(
                                    entry(2L, "baz"),
                                    entry(3L, "booze"))
                            )),
                    new HashSet<>(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }

    @Test
    public void testSliding_windowFirst_aggregate2() {
        IMap<Long, String> map = instance.getMap("source");
        // key is timestamp
        map.put(0L, "foo");
        map.put(2L, "taz");
        map.put(10L, "flush-item");

        IMap<Long, String> map2 = instance.getMap("source1");
        // key is timestamp
        map2.put(0L, "faa");
        map2.put(2L, "tuu");
        map2.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        StreamStageWithGrouping<Entry<Long, String>, Character> stage1 =
                p.drawFrom(
                        Sources.<Long, String>mapJournal("source1", START_FROM_OLDEST))
                 .addTimestamps(Entry::getKey, 0)
                 .groupingKey(entry -> entry.getValue().charAt(0));

        p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.tumbling(2))
         .groupingKey(entry -> entry.getValue().charAt(0))
         .aggregate2(stage1, toTwoBags())
         .peek()
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new TimestampedEntry<>(2, 'f', TwoBags.twoBags(
                                    asList(entry(0L, "foo")),
                                    asList(entry(0L, "faa"))
                            )),
                            new TimestampedEntry<>(4, 't', TwoBags.twoBags(
                                    asList(entry(2L, "taz")),
                                    asList(entry(2L, "tuu"))
                            )))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);

    }

    @Test
    public void testSliding_windowFirst_aggregate3() {
        IMap<Long, String> map = instance.getMap("source");
        // key is timestamp
        map.put(0L, "foo");
        map.put(2L, "taz");
        map.put(10L, "flush-item");

        IMap<Long, String> map1 = instance.getMap("source1");
        // key is timestamp
        map1.put(0L, "faa");
        map1.put(2L, "tuu");
        map1.put(10L, "flush-item");

        IMap<Long, String> map2 = instance.getMap("source2");
        // key is timestamp
        map2.put(0L, "fzz");
        map2.put(2L, "tcc");
        map2.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        StreamStageWithGrouping<Entry<Long, String>, Character> stage1 =
                p.drawFrom(
                        Sources.<Long, String>mapJournal("source1", START_FROM_OLDEST))
                 .addTimestamps(Entry::getKey, 0)
                 .groupingKey(entry -> entry.getValue().charAt(0));
        StreamStageWithGrouping<Entry<Long, String>, Character> stage2 =
                p.drawFrom(
                        Sources.<Long, String>mapJournal("source2", START_FROM_OLDEST))
                 .addTimestamps(Entry::getKey, 0)
                 .groupingKey(entry -> entry.getValue().charAt(0));

        p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.tumbling(2))
         .groupingKey(entry -> entry.getValue().charAt(0))
         .aggregate3(stage1, stage2, toThreeBags())
         .peek()
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new TimestampedEntry<>(2, 'f', ThreeBags.threeBags(
                                    asList(entry(0L, "foo")),
                                    asList(entry(0L, "faa")),
                                    asList(entry(0L, "fzz"))
                            )),
                            new TimestampedEntry<>(4, 't', ThreeBags.threeBags(
                                    asList(entry(2L, "taz")),
                                    asList(entry(2L, "tuu")),
                                    asList(entry(2L, "tcc"))
                            )))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);

    }

    @Test
    public void testSliding_windowFirst_aggregate3_with_aggregateBuilder() {
        IMap<Long, String> map = instance.getMap("source");
        // key is timestamp
        map.put(0L, "foo");
        map.put(2L, "taz");
        map.put(10L, "flush-item");

        IMap<Long, String> map1 = instance.getMap("source1");
        // key is timestamp
        map1.put(0L, "faa");
        map1.put(2L, "tuu");
        map1.put(10L, "flush-item");

        IMap<Long, String> map2 = instance.getMap("source2");
        // key is timestamp
        map2.put(0L, "fzz");
        map2.put(2L, "tcc");
        map2.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        StreamStageWithGrouping<Entry<Long, String>, Character> stage1 =
                p.drawFrom(
                        Sources.<Long, String>mapJournal("source1", START_FROM_OLDEST))
                 .addTimestamps(Entry::getKey, 0)
                 .groupingKey(entry -> entry.getValue().charAt(0));
        StreamStageWithGrouping<Entry<Long, String>, Character> stage2 =
                p.drawFrom(
                        Sources.<Long, String>mapJournal("source2", START_FROM_OLDEST))
                 .addTimestamps(Entry::getKey, 0)
                 .groupingKey(entry -> entry.getValue().charAt(0));

        WindowGroupAggregateBuilder<Entry<Long, String>, Character> b =
                p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
                 .addTimestamps(Entry::getKey, 0)
                 .window(WindowDefinition.tumbling(2))
                 .groupingKey(entry -> entry.getValue().charAt(0))
                 .aggregateBuilder();

        Tag<Entry<Long, String>> tag0 = b.tag0();
        Tag<Entry<Long, String>> tag1 = b.add(stage1);
        Tag<Entry<Long, String>> tag2 = b.add(stage2);

        b.build(AggregateOperation
                .withCreate(ThreeBags::threeBags)
                .andAccumulate(tag0, (acc, item0) -> acc.bag0().add(item0))
                .andAccumulate(tag1, (acc, item1) -> acc.bag1().add(item1))
                .andAccumulate(tag2, (acc, item2) -> acc.bag2().add(item2))
                .andCombine(ThreeBags::combineWith)
                .andDeduct(ThreeBags::deduct)
                .andFinish(ThreeBags::finish))
         .peek()
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new TimestampedEntry<>(2, 'f', ThreeBags.threeBags(
                                    asList(entry(0L, "foo")),
                                    asList(entry(0L, "faa")),
                                    asList(entry(0L, "fzz"))
                            )),
                            new TimestampedEntry<>(4, 't', ThreeBags.threeBags(
                                    asList(entry(2L, "taz")),
                                    asList(entry(2L, "tuu")),
                                    asList(entry(2L, "tcc"))
                            )))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }


    @Test
    public void testSession_windowFirst() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.session(2))
         .groupingKey(entry -> entry.getValue().charAt(0))
         .aggregate(toSet(), WindowResult::new)
         .drainTo(Sinks.list("sink"));

        testSession(p);
    }

    @Test
    public void testSession_groupingFirst() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .groupingKey(entry -> entry.getValue().charAt(0))
         .window(WindowDefinition.session(2))
         .aggregate(toSet(), WindowResult::new)
         .drainTo(Sinks.list("sink"));

        testSession(p);
    }

    private void testSession(Pipeline p) {
        IMap<Long, String> map = instance.getMap("source");
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(4L, "baz");
        map.put(5L, "booze");
        map.put(10L, "flush-item");

        instance.newJob(p);

        assertTrueEventually(() -> {
            assertEquals(
                    set(
                            new WindowResult<>(0, 2, 'f', set(entry(0L, "foo"))),
                            new WindowResult<>(1, 3, 'b', set(entry(1L, "bar"))),
                            new WindowResult<>(4, 7, 'b', set(entry(4L, "baz"),
                                    entry(5L, "booze")))),
                    new HashSet<>(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }

    @Test
    public void testSliding_groupingFirst_withNonStreamingSource() {
        IList<Entry<Long, String>> list = instance.getList("source");
        list.add(entry(0L, "foo"));
        list.add(entry(1L, "bar"));
        list.add(entry(2L, "baz"));
        list.add(entry(3L, "booze"));

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Entry<Long, String>>list("source"))
         .addTimestamps(Entry::getKey, 0)
         .groupingKey(entry -> entry.getValue().charAt(0))
         .window(WindowDefinition.tumbling(2))
         .aggregate(toSet())
         .drainTo(Sinks.list("sink"));

        instance.newJob(p).join();

        assertTrueEventually(() -> {
            assertEquals(
                    set(
                            new TimestampedEntry<>(2, 'f', set(entry(0L, "foo"))),
                            new TimestampedEntry<>(2, 'b', set(entry(1L, "bar"))),
                            new TimestampedEntry<>(4, 'b', set(
                                    entry(2L, "baz"),
                                    entry(3L, "booze"))
                            )),
                    new HashSet<>(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }
}
