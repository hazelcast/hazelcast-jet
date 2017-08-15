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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Snapshottable;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SnapshottableProcessorTaskletTest {

    private static final int MOCK_INPUT_SIZE = 10;
    private static final int CALL_COUNT_LIMIT = 10;

    private List<Object> mockInput;
    private List<InboundEdgeStream> instreams;
    private List<OutboundEdgeStream> outstreams;
    private SnapshottableProcessor processor;
    private ProcCtx context;
    private SnapshotContext snapshotContext;
    private OutboundCollector snapshotCollector;

    @Before
    public void setUp() {
        this.mockInput = IntStream.range(0, MOCK_INPUT_SIZE).boxed().collect(toList());
        this.processor = new SnapshottableProcessor();
        this.context = new ProcCtx(null, null,null, null, 0,
                true);
        this.instreams = new ArrayList<>();
        this.outstreams = new ArrayList<>();
        this.snapshotCollector = new MockOutboundCollector(10);
    }

    @Test
    public void when_isCooperative_then_true() {
        assertTrue(createTasklet().isCooperative());
    }

    @Test
    public void when_singleInstreamAndOutstream_then_outstreamGetsAll() {
        // Given
        mockInput.add(1);
        mockInput.add(2);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(mockInput, outstream1.getBuffer());
    }

    private CooperativeProcessorTasklet createTasklet() {
        snapshotContext = new SnapshotContext(ProcessingGuarantee.AT_LEAST_ONCE);
        final CooperativeProcessorTasklet t = new CooperativeProcessorTasklet(context, processor, instreams, outstreams,
                snapshotContext, null);
        t.init(new CompletableFuture<>());
        return t;
    }

    private static class SnapshottableProcessor implements Processor, Snapshottable {

        int nullaryProcessCallCountdown;
        int itemsToEmitInComplete;
        private Outbox outbox;

        private Queue<Map.Entry> snapshotQueue = new ArrayDeque<>();

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object item; (item = inbox.peek()) != null; ) {
                if (!outbox.offer(item)) {
                    return;
                } else {
                    snapshotQueue.offer(entry(UUID.randomUUID(), inbox.remove()));
                }
            }
        }

        @Override
        public boolean complete() {
            if (itemsToEmitInComplete == 0) {
                return true;
            }
            boolean accepted = outbox.offer("completing");
            if (accepted) {
                itemsToEmitInComplete--;
            }
            return itemsToEmitInComplete == 0;
        }

        @Override
        public boolean tryProcess() {
            return nullaryProcessCallCountdown-- <= 0;
        }

        @Override
        public boolean saveSnapshot() {
            for (Map.Entry item; (item = snapshotQueue.peek()) != null; ) {
                if (!outbox.offerToSnapshot(item.getKey(), item.getValue())) {
                    return false;
                } else {
                    snapshotQueue.remove();
                }
            }
            snapshotQueue.clear();
            return true;
        }

        @Override
        public void restoreSnapshotKey(Object key, Object value) {

        }
    }

    private static void callUntil(Tasklet tasklet, ProgressState expectedState) {
        int iterCount = 0;
        for (ProgressState r; (r = tasklet.call()) != expectedState; ) {
            assertTrue("Failed to make progress: " + r, r.isMadeProgress());
            assertTrue(String.format(
                    "tasklet.call() invoked %d times without reaching %s. Last state was %s",
                    CALL_COUNT_LIMIT, expectedState, r),
                    ++iterCount < CALL_COUNT_LIMIT);
        }
    }
}
