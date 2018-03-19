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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE_EDGE;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_BARRIER;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_WATERMARK;
import static com.hazelcast.jet.impl.execution.ProcessorState.END;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_INBOX;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_WATERMARK;
import static com.hazelcast.jet.impl.execution.ProcessorState.SAVE_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public class ProcessorTasklet implements Tasklet {

    private static final int OUTBOX_BATCH_SIZE = 2048;
    private final ProgressTracker progTracker = new ProgressTracker();
    private final OutboundEdgeStream[] outstreams;
    private final OutboxImpl outbox;
    private final ProcCtx context;

    private final Processor processor;
    private final SnapshotContext ssContext;
    private final BitSet receivedBarriers; // indicates if current snapshot is received on the ordinal

    private final ArrayDequeInbox inbox = new ArrayDequeInbox(progTracker);
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;
    private final WatermarkCoalescer watermarkCoalescer;

    private int numActiveOrdinals; // counter for remaining active ordinals
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private InboundEdgeStream currInstream;
    private ProcessorState state;
    private long pendingSnapshotId;
    private Watermark pendingWatermark;

    public ProcessorTasklet(@Nonnull ProcCtx context,
                            @Nonnull Processor processor,
                            @Nonnull List<? extends InboundEdgeStream> instreams,
                            @Nonnull List<? extends OutboundEdgeStream> outstreams,
                            @Nonnull SnapshotContext ssContext,
                            @Nonnull OutboundCollector ssCollector,
                            int maxWatermarkRetainMillis) {
        Preconditions.checkNotNull(processor, "processor");
        this.context = context;
        this.processor = processor;
        this.numActiveOrdinals = instreams.size();
        this.instreamGroupQueue = instreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new,
                        toCollection(ArrayList<InboundEdgeStream>::new)))
                .entrySet().stream()
                .map(Entry::getValue)
                .collect(toCollection(ArrayDeque::new));
        this.outstreams = outstreams.stream()
                                    .sorted(comparing(OutboundEdgeStream::ordinal))
                                    .toArray(OutboundEdgeStream[]::new);
        this.ssContext = ssContext;

        instreamCursor = popInstreamGroup();
        currInstream = instreamCursor != null ? instreamCursor.value() : null;
        outbox = createOutbox(ssCollector);
        receivedBarriers = new BitSet(instreams.size());
        state = initialProcessingState();
        pendingSnapshotId = ssContext.lastSnapshotId() + 1;

        watermarkCoalescer = WatermarkCoalescer.create(maxWatermarkRetainMillis, instreams.size());
    }

    private OutboxImpl createOutbox(OutboundCollector ssCollector) {
        OutboundCollector[] collectors = new OutboundCollector[outstreams.length + (ssCollector == null ? 0 : 1)];
        for (int i = 0; i < outstreams.length; i++) {
            collectors[i] = outstreams[i].getCollector();
        }
        if (ssCollector != null) {
            collectors[outstreams.length] = ssCollector;
        }
        return new OutboxImpl(collectors, ssCollector != null, progTracker,
                context.getSerializationService(), OUTBOX_BATCH_SIZE);
    }

    @Override
    public void init() {
        if (context.getSerializationService().getManagedContext() != null) {
            Object processor2 = context.getSerializationService().getManagedContext().initialize(processor);
            assert processor2 == processor : "different object returned";
        }
        processor.init(outbox, context);
    }

    @Override @Nonnull
    public ProgressState call() {
        return call(watermarkCoalescer.getTime());
    }

    // package-visible for testing
    ProgressState call(long now) {
        progTracker.reset();
        outbox.reset();
        stateMachineStep(now);
        return progTracker.toProgressState();
    }

    @SuppressWarnings("checkstyle:returncount")
    private void stateMachineStep(long now) {
        switch (state) {
            case PROCESS_WATERMARK:
                progTracker.notDone();
                if (pendingWatermark == null) {
                    long wm = watermarkCoalescer.checkWmHistory(now);
                    if (wm == NO_NEW_WM) {
                        state = PROCESS_INBOX;
                        stateMachineStep(now); // recursion
                        break;
                    }
                    pendingWatermark = new Watermark(wm);
                }
                if (pendingWatermark.equals(IDLE_MESSAGE) || processor.tryProcessWatermark(pendingWatermark)) {
                    state = EMIT_WATERMARK;
                    stateMachineStep(now); // recursion
                }
                break;

            case EMIT_WATERMARK:
                progTracker.notDone();
                if (outbox.offer(pendingWatermark)) {
                    state = PROCESS_INBOX;
                    pendingWatermark = null;
                    stateMachineStep(now); // recursion
                }
                break;

            case PROCESS_INBOX:
                progTracker.notDone();
                if (inbox.isEmpty() && (isSnapshotInbox() || processor.tryProcess())) {
                    fillInbox(now);
                }
                if (!inbox.isEmpty()) {
                    if (isSnapshotInbox()) {
                        processor.restoreFromSnapshot(inbox);
                    } else {
                        processor.process(currInstream.ordinal(), inbox);
                    }
                }

                if (inbox.isEmpty()) {
                    // there is either snapshot or instream is done, not both
                    if (currInstream != null && currInstream.isDone()) {
                        state = COMPLETE_EDGE;
                        progTracker.madeProgress();
                        return;
                    } else if (context.snapshottingEnabled()
                            && numActiveOrdinals > 0
                            && receivedBarriers.cardinality() == numActiveOrdinals) {
                        // we have an empty inbox and received the current snapshot barrier from all active ordinals
                        state = SAVE_SNAPSHOT;
                        return;
                    } else if (numActiveOrdinals == 0) {
                        progTracker.madeProgress();
                        state = COMPLETE;
                    } else {
                        state = PROCESS_WATERMARK;
                    }
                }
                return;

            case COMPLETE_EDGE:
                progTracker.notDone();
                if (isSnapshotInbox()
                        ? processor.finishSnapshotRestore() : processor.completeEdge(currInstream.ordinal())) {
                    progTracker.madeProgress();
                    state = initialProcessingState();
                }
                return;

            case SAVE_SNAPSHOT:
                assert context.snapshottingEnabled() : "Snapshotting is not enabled";

                progTracker.notDone();
                if (processor.saveToSnapshot()) {
                    progTracker.madeProgress();
                    state = EMIT_BARRIER;
                }
                return;

            case EMIT_BARRIER:
                assert context.snapshottingEnabled() : "Snapshotting is not enabled";

                progTracker.notDone();
                if (outbox.offerToEdgesAndSnapshot(new SnapshotBarrier(pendingSnapshotId))) {
                    receivedBarriers.clear();
                    pendingSnapshotId++;
                    state = initialProcessingState();
                }
                return;

            case COMPLETE:
                progTracker.notDone();
                // check ssContext to see if a barrier should be emitted
                if (context.snapshottingEnabled()) {
                    long currSnapshotId = ssContext.lastSnapshotId();
                    assert currSnapshotId <= pendingSnapshotId : "Unexpected new snapshot id " + currSnapshotId
                            + ", current was" + pendingSnapshotId;
                    if (currSnapshotId == pendingSnapshotId) {
                        state = SAVE_SNAPSHOT;
                        progTracker.madeProgress();
                        return;
                    }
                }
                if (processor.complete()) {
                    progTracker.madeProgress();
                    state = EMIT_DONE_ITEM;
                }
                return;

            case EMIT_DONE_ITEM:
                if (!outbox.offerToEdgesAndSnapshot(DONE_ITEM)) {
                    progTracker.notDone();
                    return;
                }
                state = END;
                return;

            default:
                // note ProcessorState.END goes here
                throw new JetException("Unexpected state: " + state);
        }
    }

    private void fillInbox(long now) {
        assert inbox.isEmpty() : "inbox is not empty";
        assert pendingWatermark == null : "null wm expected, but was " + pendingWatermark;

        if (instreamCursor == null) {
            return;
        }
        final InboundEdgeStream first = instreamCursor.value();
        ProgressState result;
        do {
            currInstream = instreamCursor.value();
            result = NO_PROGRESS;

            // skip ordinals where a snapshot barrier has already been received
            if (ssContext != null && ssContext.processingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE
                    && receivedBarriers.get(currInstream.ordinal())) {
                instreamCursor.advance();
                continue;
            }
            result = currInstream.drainTo(inbox.queue()::add);
            progTracker.madeProgress(result.isMadeProgress());

            // check if the last drained item is special
            Object lastItem = inbox.queue().peekLast();
            if (lastItem instanceof Watermark) {
                long newWmValue = ((Watermark) inbox.queue().removeLast()).timestamp();
                long wm = watermarkCoalescer.observeWm(now, currInstream.ordinal(), newWmValue);
                if (wm != NO_NEW_WM) {
                    pendingWatermark = new Watermark(wm);
                }
            } else if (lastItem instanceof SnapshotBarrier) {
                SnapshotBarrier barrier = (SnapshotBarrier) inbox.queue().removeLast();
                observeSnapshot(currInstream.ordinal(), barrier.snapshotId());
            } else if (lastItem != null && !(lastItem instanceof BroadcastItem)) {
                watermarkCoalescer.observeEvent(currInstream.ordinal());
            }

            if (result.isDone()) {
                receivedBarriers.clear(currInstream.ordinal());
                long wm = watermarkCoalescer.queueDone(currInstream.ordinal());
                // Note that there can be a WM received from upstream and the result can be done after single drain.
                // In this case we might overwrite the WM here, but that's fine since the second WM should be newer.
                if (wm != NO_NEW_WM) {
                    assert pendingWatermark == null || pendingWatermark.timestamp() < wm
                            : "trying to assign lower WM. Old=" + pendingWatermark.timestamp() + ", new=" + wm;
                    pendingWatermark = new Watermark(wm);
                }
                instreamCursor.remove();
                numActiveOrdinals--;
            }

            // pop current priority group
            if (!instreamCursor.advance()) {
                instreamCursor = popInstreamGroup();
                return;
            }
        } while (!result.isMadeProgress() && instreamCursor.value() != first);
    }

    private CircularListCursor<InboundEdgeStream> popInstreamGroup() {
        return Optional.ofNullable(instreamGroupQueue.poll())
                       .map(CircularListCursor::new)
                       .orElse(null);
    }

    @Override
    public String toString() {
        return "ProcessorTasklet{" + context.vertexName() + '#' + context.globalProcessorIndex() + '}';
    }

    private void observeSnapshot(int ordinal, long snapshotId) {
        if (snapshotId != pendingSnapshotId) {
            throw new JetException("Unexpected snapshot barrier " + snapshotId + " from ordinal " + ordinal +
                    " expected " + pendingSnapshotId);
        }
        receivedBarriers.set(ordinal);
    }

    /**
     * Initial state of the processor. If there are no inbound ordinals left, we will go to COMPLETE state
     * otherwise to PROCESS_INBOX.
     */
    private ProcessorState initialProcessingState() {
        return pendingWatermark != null ? PROCESS_WATERMARK
                : instreamCursor == null ? COMPLETE : PROCESS_INBOX;
    }

    /**
     * Returns, if the inbox we are currently on is the snapshot restoring inbox.
     */
    private boolean isSnapshotInbox() {
        return currInstream != null && currInstream.priority() == Integer.MIN_VALUE;
    }

    @Override
    public boolean isCooperative() {
        return processor.isCooperative();
    }
}
