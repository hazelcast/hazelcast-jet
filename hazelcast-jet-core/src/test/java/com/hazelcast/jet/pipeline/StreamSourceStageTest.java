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

import com.hazelcast.core.IMap;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

@RunWith(HazelcastSerialClassRunner.class)
public class StreamSourceStageTest extends StreamSourceStageTestBase {

    @BeforeClass
    public static void beforeClass1() {
        IMap<Integer, Integer> sourceMap = instances[0].getMap(JOURNALED_MAP_NAME);
        sourceMap.put(1, 1);
        sourceMap.put(2, 2);
    }

    private static StreamSource<Integer> createSourceJournal() {
        return Sources.<Integer, Integer, Integer>mapJournal(JOURNALED_MAP_NAME, alwaysTrue(),
                EventJournalMapEvent::getKey, START_FROM_OLDEST);
    }

    private static StreamSource<Integer> createSourceBuilder() {
        return SourceBuilder.timestampedStream("s", ctx -> new boolean[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (!ctx[0]) {
                        buf.add(1, 1);
                        buf.add(2, 2);
                        ctx[0] = true;
                    }
                })
                .build();
    }

    @Test
    public void test_sourceBuilder_withoutTimestamps() {
        test(createSourceBuilder(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceBuilder_withDefaultTimestamps() {
        test(createSourceBuilder(), withDefaultTimestampsFn, asList(1L, 2L), null);
    }

    @Test
    public void test_sourceBuilder_withTimestamps() {
        test(createSourceBuilder(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceJournal_withoutTimestamps() {
        test(createSourceJournal(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceJournal_withDefaultTimestamps() {
        test(createSourceJournal(), withDefaultTimestampsFn, emptyList(),
                "Neither timestampFn nor defaultEventTime specified");
    }

    @Test
    public void test_sourceJournal_withTimestamps() {
        test(createSourceJournal(), withTimestampsFn, asList(2L, 3L), null);
    }
}
