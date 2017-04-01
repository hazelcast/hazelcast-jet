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

import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.impl.util.ProgressState;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.execution.ExecutionService.IDLER;
import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;

/**
 * Tasklet that drives a non-cooperative processor.
 */
public class BlockingProcessorTasklet extends ProcessorTaskletBase {

    CompletableFuture jobFuture;
    private final BlockingOutbox outbox;

    public BlockingProcessorTasklet(
            String vertexName, Context context, Processor processor,
            List<InboundEdgeStream> instreams, List<OutboundEdgeStream> outstreams
    ) {
        super(vertexName, context, processor, instreams, outstreams);
        outbox = new BlockingOutbox();
        processor.init(outbox, context);
    }

    @Override
    public final boolean isCooperative() {
        return false;
    }

    @Override @Nonnull
    public ProgressState call() {
        try {
            progTracker.reset();
            tryFillInbox();
            if (progTracker.isDone()) {
                complete();
            } else if (!inbox().isEmpty()) {
                process();
            } else {
                System.out.println(processor + "inbox empty " + progTracker);
            }
            return progTracker.toProgressState();
        } catch (JobFutureCancelled e) {
            return ProgressState.DONE;
        }
    }

    private void process() {
        progTracker.madeProgress();
        processor.process(currInstream.ordinal(), inbox());
    }

    private void complete() {
        progTracker.madeProgress();
        if (processor.complete()) {
            outbox.add(DONE_ITEM);
        } else {
            progTracker.notDone();
        }
    }

    private class BlockingOutbox implements Outbox {

        @Override
        public int bucketCount() {
            return outstreams.length;
        }

        @Override
        public boolean isHighWater(int ordinal) {
            return false;
        }

        @Override
        public void add(int ordinal, @Nonnull Object item) {
            if (ordinal != -1) {
                submit(outstreams[ordinal], item);
            } else {
                for (OutboundEdgeStream outstream : outstreams) {
                    submit(outstream, item);
                }
            }
        }

        private void submit(OutboundEdgeStream outstream, @Nonnull Object item) {
            OutboundCollector collector = outstream.getCollector();
            for (long idleCount = 0; ;) {
                ProgressState result = (item != DONE_ITEM) ? collector.offer(item) : collector.close();
                if (result.isDone()) {
                    return;
                }
                if (jobFuture.isCompletedExceptionally()) {
                    throw new JobFutureCancelled();
                }
                if (result.isMadeProgress()) {
                    idleCount = 0;
                } else {
                    IDLER.idle(++idleCount);
                }
            }
        }
    }

    private static class JobFutureCancelled extends RuntimeException {
    }
}
