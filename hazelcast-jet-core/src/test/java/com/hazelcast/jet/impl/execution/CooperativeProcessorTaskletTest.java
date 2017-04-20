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
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class CooperativeProcessorTaskletTest {

    private static final int MOCK_INPUT_SIZE = 10;
    private static final int CALL_COUNT_LIMIT = 10;
    private List<Object> mockInput;
    private List<InboundEdgeStream> instreams;
    private List<OutboundEdgeStream> outstreams;
    private PassThroughProcessor processor;
    private ProcCtx context;

    @Before
    public void setUp() {
        this.mockInput = IntStream.range(0, MOCK_INPUT_SIZE).boxed().collect(toList());
        this.processor = new PassThroughProcessor();
        this.context = new ProcCtx(null, null, null, 0);
        this.instreams = new ArrayList<>();
        this.outstreams = new ArrayList<>();
    }

    @Test
    public void when_isCooperative_then_true() {
        assertTrue(createTasklet().isCooperative());
    }

    @Test
    public void when_singleInstreamAndOutstream_then_outstreamGetsAll() {
        // Given
        mockInput.add(DONE_ITEM);
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

    @Test
    public void when_oneInstreamAndTwoOutstreams_then_allOutstreamsGetAllItems() {
        // Given
        mockInput.add(DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        MockOutboundStream outstream2 = new MockOutboundStream(1);
        instreams.add(instream1);
        outstreams.add(outstream1);
        outstreams.add(outstream2);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(mockInput, outstream1.getBuffer());
        assertEquals(mockInput, outstream2.getBuffer());
    }

    @Test
    public void when_instreamChunked_then_processAllEventually() {
        // Given
        mockInput.add(DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, 4);
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(mockInput, outstream1.getBuffer());
    }

    @Test
    public void when_3instreams_then_pushAllIntoOutstream() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, mockInput.subList(0, 4), 4);
        MockInboundStream instream2 = new MockInboundStream(1, mockInput.subList(4, 8), 4);
        MockInboundStream instream3 = new MockInboundStream(2, mockInput.subList(8, 10), 4);
        instream1.push(DONE_ITEM);
        instream2.push(DONE_ITEM);
        instream3.push(DONE_ITEM);
        instreams.addAll(asList(instream1, instream2, instream3));
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        mockInput.add(DONE_ITEM);
        assertEquals(new HashSet<>(mockInput), new HashSet<>(outstream1.getBuffer()));
    }

    @Test
    public void when_outstreamRefusesItem_then_noProgress() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertTrue(outstream1.getBuffer().equals(mockInput.subList(0, 1)));
    }

    @Test
    public void when_inboxEmpty_then_nullaryProcessCalled() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, emptyList(), 1);
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        CooperativeProcessorTasklet tasklet = createTasklet();
        processor.nullaryProcessCallCountdown = 1;

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertTrue(processor.nullaryProcessCallCountdown <= 0);
    }

    @Test
    public void when_completeReturnsFalse_then_retried() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, emptyList(), 1);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);
        CooperativeProcessorTasklet tasklet = createTasklet();
        processor.itemsToEmitInComplete = 2;

        // When

        // first call doesn't immediately detect there is no input
        callUntil(tasklet, NO_PROGRESS);
        // first "completing" item can't be flushed from outbox due to outstream1 constraint
        callUntil(tasklet, NO_PROGRESS);
        outstream1.flush();
        // second "completing" item can't be flushed from outbox due to outstream1 constraint
        callUntil(tasklet, NO_PROGRESS);
        outstream1.flush();
        callUntil(tasklet, DONE);

        // Then
        assertTrue(processor.itemsToEmitInComplete <= 0);

    }

    private CooperativeProcessorTasklet createTasklet() {
        final CooperativeProcessorTasklet t = new CooperativeProcessorTasklet(
                context, processor, instreams, outstreams);
        t.init(new CompletableFuture<>());
        return t;
    }

    private static class PassThroughProcessor implements Processor {
        private Outbox outbox;
        int nullaryProcessCallCountdown;
        int itemsToEmitInComplete;

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object item; (item = inbox.poll()) != null; ) {
                if (!outbox.offer(item)) {
                    return;
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
