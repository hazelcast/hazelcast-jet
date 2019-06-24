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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSourcesTest extends JetTestSupport {

    private static final String SINK_NAME = "results";

    private JetInstance jet;
    private IListJet<Object> sinkList;

    @Before
    public void setup() {
        jet = createJetMember();

        sinkList = jet.getList(SINK_NAME);
    }

    @Test
    public void test_items() {
        Pipeline p = Pipeline.create();

        Object[] input = IntStream.range(0, 10_000).boxed().toArray();
        p.drawFrom(TestSources.items(input))
         .drainTo(Sinks.list(SINK_NAME));

        jet.newJob(p).join();

        assertEquals(Arrays.asList(input), new ArrayList<>(sinkList));
    }

    @Test
    public void test_itemStream() {
        Pipeline p = Pipeline.create();

        p.drawFrom(TestSources.itemStream(10))
         .withoutTimestamps()
         .drainTo(Sinks.list(SINK_NAME))
         .setLocalParallelism(1);

        jet.newJob(p);

        assertTrueEventually(() -> {
            int count = 20;
            assertTrue("list should contain at least " + count + " items", sinkList.size() >= count);
            List<Object> items = sinkList.subList(0, count);
            for (int i = 0; i < count; i++) {
                SimpleEvent e = (SimpleEvent) items.get(i);
                assertEquals(i, e.sequence());
            }
        }, 10);
    }

    @Test
    public void test_itemStream_withWindowing() {
        Pipeline p = Pipeline.create();

        int itemsPerSecond = 10;
        p.drawFrom(TestSources.itemStream(itemsPerSecond))
         .withNativeTimestamps(0)
         .window(WindowDefinition.tumbling(1000))
         .aggregate(AggregateOperations.counting())
         .drainTo(Sinks.list(SINK_NAME));

        jet.newJob(p);

        assertTrueEventually(() -> {
            assertTrue("sink list should contain some items", sinkList.size() > 1);
            // first window may be in complete, subsequent windows should have 10 items
            WindowResult<Long> items = (WindowResult<Long>) sinkList.get(1);
            assertEquals(10L, (long)items.result());
        }, 10);
    }
}
