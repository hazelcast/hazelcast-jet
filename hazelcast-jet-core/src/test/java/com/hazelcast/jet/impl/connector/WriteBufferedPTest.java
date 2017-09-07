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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.processor.SinkProcessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class WriteBufferedPTest extends JetTestSupport {

    private static final List<String> events = new CopyOnWriteArrayList<>();

    @Before
    public void setup() {
        events.clear();
    }

    @Test
    public void writeBuffered_smokeTest() throws Exception {
        Processor p = getLoggingBufferedWriter().get(1).iterator().next();
        Outbox outbox = mock(Outbox.class);
        p.init(outbox, mock(Context.class));
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        inbox.add(1);
        inbox.add(2);
        p.process(0, inbox);
        inbox.add(3);
        inbox.add(4);
        inbox.add(new Watermark(0)); // watermark should not be written
        p.process(0, inbox);
        p.process(0, inbox); // empty flush
        p.complete();

        assertEquals(asList(
                "new",
                "add:1",
                "add:2",
                "flush",
                "add:3",
                "add:4",
                "flush",
                "flush",
                "dispose"
        ), events);

        assertEquals(0, inbox.size());
        verifyZeroInteractions(outbox);
    }

    @Test
    public void when_writeBufferedJobFailed_then_bufferDisposed() throws Exception {
        JetInstance instance = createJetMember();
        try {
            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", StuckSource::new);
            Vertex sink = dag.newVertex("sink", getLoggingBufferedWriter()).localParallelism(1);

            dag.edge(Edge.between(source, sink));

            Job job = instance.newJob(dag);
            // wait for the job to initialize
            Thread.sleep(5000);
            job.cancel();

            assertTrueEventually(() -> assertTrue("No \"dispose\", only: " + events, events.contains("dispose")), 60);
            System.out.println(events);
        } finally {
            instance.shutdown();
        }
    }

    // returns a processor that will not write anywhere, just log the events
    private static ProcessorSupplier getLoggingBufferedWriter() {
        return SinkProcessors.writeBuffered(
                idx -> {
                    events.add("new");
                    return null;
                },
                (buffer, item) -> events.add("add:" + item),
                buffer -> events.add("flush"),
                buffer -> events.add("dispose")
        );
    }

    private static class StuckSource extends AbstractProcessor {
        StuckSource() {
            setCooperative(false);
        }

        @Override
        public boolean complete() {
            try {
                currentThread().join();
            } catch (InterruptedException ignored) {
            }
            return false;
        }
    }
}
