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
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.execution.ExecutionService.IDLER;

/**
 * Tasklet that drives a non-cooperative processor.
 */
public class BlockingProcessorTasklet extends ProcessorTaskletBase {

    private CompletableFuture<?> jobFuture;
    private BlockingSnapshotStorageImpl snapshotStorage;

    public BlockingProcessorTasklet(
            ProcCtx context, Processor processor, List<InboundEdgeStream> instreams,
            List<OutboundEdgeStream> outstreams, SnapshotState snapshotState,
            Queue<Object> snapshotQueue, SerializationService serializationService
    ) {
        super(context, processor, instreams, outstreams, snapshotState, snapshotQueue, serializationService);
        Preconditions.checkFalse(processor.isCooperative(), "Processor is cooperative");
        outbox = new BlockingOutbox();
    }

    @Override
    public final boolean isCooperative() {
        return false;
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        super.init(jobFuture);
        this.jobFuture = jobFuture;
        if (snapshotStorage != null) {
            snapshotStorage.initJobFuture(jobFuture);
        }
    }

    @Override @Nonnull
    public ProgressState call() {
        try {
            return super.call();
        } catch (JobFutureCompleted e) {
            return ProgressState.DONE;
        }
    }

    @Override
    protected SnapshotStorageImpl createSnapshotStorage(Queue<Object> snapshotQueue, SerializationService serializationService) {
        return snapshotStorage = new BlockingSnapshotStorageImpl(serializationService, snapshotQueue);
    }

    @Override
    protected void tryFlushOutbox() {
        // do nothing: our outbox flushes automatically
    }

    private class BlockingOutbox implements Outbox {

        @Override
        public int bucketCount() {
            return outstreams.length;
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            progTracker.madeProgress();
            if (ordinal != -1) {
                submit(outstreams[ordinal], item);
            } else {
                for (OutboundEdgeStream outstream : outstreams) {
                    submit(outstream, item);
                }
            }
            return true;
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            progTracker.madeProgress();
            for (int ord : ordinals) {
                submit(outstreams[ord], item);
            }
            return true;
        }

        private void submit(OutboundEdgeStream outstream, @Nonnull Object item) {
            OutboundCollector collector = outstream.getCollector();
            for (long idleCount = 0; ;) {
                ProgressState result = (item instanceof Watermark || item instanceof DoneItem
                            || item instanceof SnapshotBarrier)
                        ? collector.offerBroadcast(item)
                        : collector.offer(item);
                if (result.isDone()) {
                    return;
                }
                if (jobFuture.isDone()) {
                    throw new JobFutureCompleted();
                }
                if (result.isMadeProgress()) {
                    idleCount = 0;
                } else {
                    IDLER.idle(++idleCount);
                }
            }
        }
    }

    static class JobFutureCompleted extends RuntimeException {
    }
}
