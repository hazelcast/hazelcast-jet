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
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Preconditions;

import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * Tasklet that drives a cooperative processor.
 */
public class CooperativeProcessorTasklet extends ProcessorTaskletBase {

    public CooperativeProcessorTasklet(ProcCtx context, Processor processor,
                                       List<InboundEdgeStream> instreams, List<OutboundEdgeStream> outstreams,
                                       SnapshotState snapshotState, Queue<Object> snapshotQueue,
                                       SerializationService serializationService,
                                       ProcessingGuarantee processingGuarantee) {
        super(context, processor, instreams, outstreams, snapshotState, snapshotQueue, serializationService,
                processingGuarantee);
        Preconditions.checkTrue(processor.isCooperative(), "Processor is non-cooperative");
        int[] bucketCapacities = Stream.of(this.outstreams).mapToInt(OutboundEdgeStream::getOutboxCapacity).toArray();
        outbox = new ArrayDequeOutbox(bucketCapacities, progTracker);
    }

    @Override
    protected SnapshotStorageImpl createSnapshotStorage(Queue<Object> snapshotQueue,
                                                        SerializationService serializationService) {
        return new SnapshotStorageImpl(serializationService, snapshotQueue);
    }

    @Override
    protected void tryFlushOutbox() {
        nextOutstream:
        for (int i = 0; i < outbox.bucketCount(); i++) {
            final Queue q = ((ArrayDequeOutbox) outbox).queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                final OutboundCollector c = outstreams[i].getCollector();
                final ProgressState state =
                        (item instanceof Watermark || item instanceof DoneItem || item instanceof SnapshotBarrier
                                ? c.offerBroadcast(item) : c.offer(item));
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

