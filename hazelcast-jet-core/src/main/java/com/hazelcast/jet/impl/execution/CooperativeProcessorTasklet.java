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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;

/**
 * Tasklet that drives a cooperative processor.
 */
public class CooperativeProcessorTasklet extends ProcessorTaskletBase {
    private final ArrayDequeOutbox outbox;
    private boolean processorCompleted;

    public CooperativeProcessorTasklet(ProcCtx context, Processor processor,
                                       List<InboundEdgeStream> instreams, List<OutboundEdgeStream> outstreams) {
        super(context, processor, instreams, outstreams);
        Preconditions.checkTrue(processor.isCooperative(), "Processor is non-cooperative");
        int[] bucketCapacities = Stream.of(this.outstreams).mapToInt(OutboundEdgeStream::getOutboxCapacity).toArray();
        this.outbox = new ArrayDequeOutbox(bucketCapacities, progTracker);
    }

    @Override
    public final boolean isCooperative() {
        return true;
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        initProcessor(outbox, jobFuture);
    }

    @Override @Nonnull
    public ProgressState call() {
        progTracker.reset();
        if (!inbox().isEmpty()) {
            progTracker.notDone();
        } else {
            if (!processor.tryProcess()) {
                tryFlushOutbox();
                progTracker.notDone();
                return progTracker.toProgressState();
            }
            tryFillInbox();
        }
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox().isEmpty()) {
            processor.process(currInstream.ordinal(), inbox());
        }
        tryFlushOutbox();
        return progTracker.toProgressState();
    }

    private void completeIfNeeded() {
        if (processorCompleted) {
            return;
        }
        processorCompleted = processor.complete();
        if (processorCompleted) {
            outbox.addIgnoringCapacity(DONE_ITEM);
            return;
        }
        progTracker.notDone();
    }

    private void tryFlushOutbox() {
        nextOutstream:
        for (int i = 0; i < outbox.bucketCount(); i++) {
            final Queue q = outbox.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                final OutboundCollector c = outstreams[i].getCollector();
                final ProgressState state = (item instanceof Punctuation || item instanceof DoneItem ?
                        c.offerBroadcast(item) : c.offer(item));
                progTracker.madeProgress(state.isMadeProgress());
                if (!state.isDone()) {
                    progTracker.notDone();
                    continue nextOutstream;
                }
                q.remove();
            }
        }
    }
}

