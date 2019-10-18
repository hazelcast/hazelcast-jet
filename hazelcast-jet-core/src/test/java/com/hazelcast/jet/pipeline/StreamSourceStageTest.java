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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.map.IMap;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class StreamSourceStageTest extends StreamSourceStageTestBase {

    @BeforeClass
    public static void beforeClass1() {
        IMap<Integer, Integer> sourceMap = instance.getMap(JOURNALED_MAP_NAME);
        sourceMap.put(1, 1);
        sourceMap.put(2, 2);
    }

    private static StreamSource<Integer> createSourceJournal() {
        return Sources.<Integer, Integer, Integer>mapJournal(JOURNALED_MAP_NAME, alwaysTrue(),
                EventJournalMapEvent::getKey, START_FROM_OLDEST);
    }

    private static StreamSource<Integer> createTimestampedSourceBuilder() {
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

    private static StreamSource<Integer> createPlainSourceBuilder() {
        return SourceBuilder.stream("s", ctx -> new boolean[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (!ctx[0]) {
                        buf.add(1);
                        buf.add(2);
                        ctx[0] = true;
                    }
                })
                .build();
    }

    @Test
    public void test_timestampedSourceBuilder_withoutTimestamps() {
        test(createTimestampedSourceBuilder(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_timestampedSourceBuilder_withNativeTimestamps() {
        test(createTimestampedSourceBuilder(), withNativeTimestampsFn, asList(1L, 2L), null);
    }

    @Test
    public void test_timestampedSourceBuilder_withTimestamps() {
        test(createTimestampedSourceBuilder(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_plainSourceBuilder_withoutTimestamps() {
        test(createPlainSourceBuilder(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_plainSourceBuilder_withNativeTimestamps() {
        test(createPlainSourceBuilder(), withNativeTimestampsFn, emptyList(),
                "The source doesn't support native timestamps");
    }

    @Test
    public void test_plainSourceBuilder_withTimestamps() {
        test(createPlainSourceBuilder(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceJournal_withoutTimestamps() {
        test(createSourceJournal(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceJournal_withNativeTimestamps() {
        test(createSourceJournal(), withNativeTimestampsFn, emptyList(),
                "The source doesn't support native timestamps");
    }

    @Test
    public void test_sourceJournal_withTimestamps() {
        test(createSourceJournal(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_withTimestampsButTimestampsNotUsed() {
        IList sinkList = instance.getList("sinkList");

        Pipeline p = Pipeline.create();
        p.drawFrom(createSourceJournal())
         .withIngestionTimestamps()
         .drainTo(Sinks.list(sinkList));

        instance.newJob(p);
        assertTrueEventually(() -> assertEquals(Arrays.asList(1, 2), new ArrayList<>(sinkList)), 5);
    }

    @Test
    public void when_withTimestampsAndAddTimestamps_then_fail() {
        Pipeline p = Pipeline.create();
        StreamStage<Entry<Object, Object>> stage = p.drawFrom(Sources.mapJournal("foo", START_FROM_OLDEST))
                                                  .withIngestionTimestamps();

        expectedException.expectMessage("This stage already has timestamps assigned to it");
        stage.addTimestamps(o -> 0L, 0);
    }

    @Test
    public void when_sourceHasPreferredLocalParallelism_then_lpMatchSource() {
        // Given
        int lp = 11;

        // When
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.streamFromProcessor("src",
                ProcessorMetaSupplier.of(lp, ProcessorSupplier.of(noopP()))))
         .withTimestamps(o -> 0L, 0)
         .drainTo(Sinks.noop());
        DAG dag = p.toDag();

        // Then
        Vertex srcVertex = dag.getVertex("src");
        Vertex tsVertex = dag.getVertex("src-add-timestamps");
        assertEquals(lp, srcVertex.determineLocalParallelism(-1));
        assertEquals(lp,  tsVertex.determineLocalParallelism(-1));
}

    @Test
    public void when_sourceHasExplicitLocalParallelism_then_lpMatchSource() {
        // Given
        int lp = 11;

        // When
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.streamFromProcessor("src",
                ProcessorMetaSupplier.of(ProcessorSupplier.of(noopP()))))
         .withTimestamps(o -> 0L, 0)
         .setLocalParallelism(lp)
         .drainTo(Sinks.noop());
        DAG dag = p.toDag();

        // Then
        Vertex srcVertex = dag.getVertex("src");
        Vertex tsVertex = dag.getVertex("src-add-timestamps");
        assertEquals(lp, srcVertex.getLocalParallelism());
        assertEquals(lp, tsVertex.getLocalParallelism());
    }


    @Test
    public void when_sourceHasNoPreferredLocalParallelism_then_lpMatchSource() {
        // Given
        StreamSource<Object> source = Sources.streamFromProcessor("src",
                ProcessorMetaSupplier.of(ProcessorSupplier.of(noopP())));

        // When
        Pipeline p = Pipeline.create();
        p.drawFrom(source)
         .withTimestamps(o -> 0L, 0)
         .drainTo(Sinks.noop());
        DAG dag = p.toDag();

        // Then
        Vertex srcVertex = dag.getVertex("src");
        Vertex tsVertex = dag.getVertex("src-add-timestamps");
        int lp1 = srcVertex.determineLocalParallelism(-1);
        int lp2 = tsVertex.determineLocalParallelism(-1);
        assertEquals(lp1, lp2);
    }
}
