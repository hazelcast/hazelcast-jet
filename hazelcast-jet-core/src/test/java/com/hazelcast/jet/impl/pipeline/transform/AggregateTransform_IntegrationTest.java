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
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.core.TestUtil.set;
import static com.hazelcast.jet.core.test.TestSupport.listToString;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class AggregateTransform_IntegrationTest extends JetTestSupport {

    private JetInstance instance;

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(
                new EventJournalConfig().setMapName("source").setEnabled(true));
        config.getHazelcastConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), "6");
        instance = createJetMember(config);
    }

    @Test
    public void test() {
        IMap<Long, String> map = instance.getMap("source");
        map.put(0L, "foo");
        map.put(1L, "bar");

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>map("source"))
         .aggregate(toSet())
         .drainTo(Sinks.list("sink"));

        instance.newJob(p).join();

        assertEquals(
                listToString(singletonList(set(entry(0L, "foo"), entry(1L, "bar")))),
                listToString(instance.getHazelcastInstance().getList("sink")));
    }
}
