/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JournalInitialPosition;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.core.TestUtil.set;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(ParallelTest.class)
public class WindowGroupTransform_IntegrationTest extends JetTestSupport {

    @Parameter
    public boolean singleStage;

    private JetInstance instance;

    @Parameters(name = "singleStage={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(
                new EventJournalConfig().setMapName("source").setEnabled(true));
        instance = createJetMember(config);
    }

    @Test
    public void testSliding_groupingFirst() {
        Pipeline p = Pipeline.create();
        StreamStage<TimestampedEntry<Character, Set<Entry<Long, String>>>> stage =
                p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
                 .setTimestampWithEventTime(Entry::getKey, 0)
                 .groupingKey(entry -> entry.getValue().charAt(0))
                 .window(WindowDefinition.tumbling(2))
                 .aggregate(toSet());
        if (singleStage) {
            stage = stage.setOptimizeMemory();
        }
        stage.drainTo(Sinks.list("sink"));

        testSliding(p);
    }

    @Test
    public void testSliding_windowFirst() {
        Pipeline p = Pipeline.create();
        StreamStage<TimestampedEntry<Character, Set<Entry<Long, String>>>> stage =
                p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
                 .setTimestampWithEventTime(Entry::getKey, 0)
                 .window(WindowDefinition.tumbling(2))
                 .groupingKey(entry -> entry.getValue().charAt(0))
                 .aggregate(toSet());
        if (singleStage) {
            stage = stage.setOptimizeMemory();
        }
        stage.drainTo(Sinks.list("sink"));

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
    public void testSession_windowFirst() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
         .setTimestampWithEventTime(Entry::getKey, 0)
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
         .setTimestampWithEventTime(Entry::getKey, 0)
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
                            new WindowResult<>(4, 7, 'b', set(entry(4L, "baz"), entry(5L, "booze")))),
                    new HashSet<>(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }
}
