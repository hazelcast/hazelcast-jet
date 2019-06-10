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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeBuilder;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.function.Predicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE_EDGE;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_BARRIER;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.END;
import static com.hazelcast.jet.impl.execution.ProcessorState.FINAL_ON_SNAPSHOT_COMPLETED;
import static com.hazelcast.jet.impl.execution.ProcessorState.ON_SNAPSHOT_COMPLETED;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_INBOX;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_WATERMARK;
import static com.hazelcast.jet.impl.execution.ProcessorState.SAVE_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.ProcessorState.WAITING_FOR_SNAPSHOT_COMPLETED;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static com.hazelcast.jet.impl.util.Util.jobNameAndExecutionId;
import static com.hazelcast.jet.impl.util.Util.lazyAdd;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.sum;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public class ProcessorTasklet implements Tasklet {

    private static final int OUTBOX_BATCH_SIZE = 2048;

    private final ProgressTracker progTracker = new ProgressTracker();
    private final OutboundEdgeStream[] outstreams;
    private final OutboxImpl outbox;
    private final Processor.Context context;

    private final Processor processor;
    private final SnapshotContext ssContext;
    private final BitSet receivedBarriers; // indicates if current snapshot is received on the ordinal

    private final ArrayDequeInbox inbox = new ArrayDequeInbox(progTracker);
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;
    private final WatermarkCoalescer watermarkCoalescer;
    private final ILogger logger;
    private final SerializationService serializationService;

    private int numActiveOrdinals; // counter for remaining active ordinals
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private InboundEdgeStream currInstream;
    private ProcessorState state;
    private long pendingSnapshotId1;
    private long pendingSnapshotId2;
    private SnapshotBarrier currentBarrier;
    private Watermark pendingWatermark;
    private boolean processorClosed;

    // Tells whether we are operating in exactly-once or at-least-once mode.
    // In other words, whether a barrier from all inputs must be present before
    // draining more items from an input stream where a barrier has been reached.
    // Once a terminal snapshot barrier is reached, this is always true.
    private boolean waitForAllBarriers;

    private final AtomicLongArray receivedCounts;
    private final AtomicLongArray receivedBatches;
    private final AtomicLongArray emittedCounts;
    private final AtomicLong queuesSize = new AtomicLong();
    private final AtomicLong queuesCapacity = new AtomicLong();
    private final Predicate<Object> addToInboxFunction = inbox.queue()::add;

    @SuppressWarnings("checkstyle:ExecutableStatementCount")
    public ProcessorTasklet(@Nonnull Context context,
                            @Nonnull SerializationService serializationService,
                            @Nonnull Processor processor,
                            @Nonnull List<? extends InboundEdgeStream> instreams,
                            @Nonnull List<? extends OutboundEdgeStream> outstreams,
                            @Nonnull SnapshotContext ssContext,
                            @Nonnull OutboundCollector ssCollector,
                            @Nullable ProbeBuilder probeBuilder
    ) {
        Preconditions.checkNotNull(processor, "processor");
        this.context = context;
        this.serializationService = serializationService;
        this.processor = processor;
        this.numActiveOrdinals = instreams.size();
        this.instreamGroupQueue = new ArrayDeque<>(instreams.stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new,
                        toCollection(ArrayList<InboundEdgeStream>::new)))
                .values());
        this.outstreams = outstreams.stream()
                                    .sorted(comparing(OutboundEdgeStream::ordinal))
                                    .toArray(OutboundEdgeStream[]::new);
        this.ssContext = ssContext;
        this.logger = getLogger(context);

        instreamCursor = popInstreamGroup();
        receivedCounts = new AtomicLongArray(instreams.size());
        receivedBatches = new AtomicLongArray(instreams.size());
        emittedCounts = new AtomicLongArray(outstreams.size() + 1);
        outbox = createOutbox(ssCollector);
        receivedBarriers = new BitSet(instreams.size());
        state = processingState();
        pendingSnapshotId1 = pendingSnapshotId2 = ssContext.activeSnapshotId1stPhase() + 1;
        waitForAllBarriers = ssContext.processingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE;

        watermarkCoalescer = WatermarkCoalescer.create(instreams.size());
        if (probeBuilder != null) {
            registerMetrics(instreams, probeBuilder);
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
            justification = "jetInstance() can be null in TestProcessorContext")
    private ILogger getLogger(@Nonnull Context context) {
        return context.jetInstance() != null
                ? context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(getClass() + "." + toString())
                : Logger.getLogger(getClass());
    }

    private void registerMetrics(List<? extends InboundEdgeStream> instreams, final ProbeBuilder probeBuilder) {
        for (int i = 0; i < instreams.size(); i++) {
            int finalI = i;
            ProbeBuilder builderWithOrdinal = probeBuilder
                    .withTag("ordinal", String.valueOf(i));
            builderWithOrdinal.register(this, "receivedCount", ProbeLevel.INFO, ProbeUnit.COUNT,
                            (LongProbeFunction<ProcessorTasklet>) t -> t.receivedCounts.get(finalI));
            builderWithOrdinal.register(this, "receivedBatches", ProbeLevel.INFO, ProbeUnit.COUNT,
                            (LongProbeFunction<ProcessorTasklet>) t -> t.receivedBatches.get(finalI));

            InboundEdgeStream instream = instreams.get(finalI);
            builderWithOrdinal.register(this, "topObservedWm", ProbeLevel.INFO, ProbeUnit.MS,
                    (LongProbeFunction<ProcessorTasklet>) t -> instream.topObservedWm());
            builderWithOrdinal.register(this, "coalescedWm", ProbeLevel.INFO, ProbeUnit.MS,
                    (LongProbeFunction<ProcessorTasklet>) t -> instream.coalescedWm());
        }

        for (int i = 0; i < emittedCounts.length() - (context.snapshottingEnabled() ? 0 : 1); i++) {
            int finalI = i;
            probeBuilder
                    .withTag("ordinal", i == emittedCounts.length() - 1 ? "snapshot" : String.valueOf(i))
                    .register(this, "emittedCount", ProbeLevel.INFO, ProbeUnit.COUNT,
                            (LongProbeFunction<ProcessorTasklet>) t -> t.emittedCounts.get(finalI));
        }

        probeBuilder.register(this, "topObservedWm", ProbeLevel.INFO, ProbeUnit.MS,
                (LongProbeFunction<ProcessorTasklet>) t -> t.watermarkCoalescer.topObservedWm());
        probeBuilder.register(this, "coalescedWm", ProbeLevel.INFO, ProbeUnit.MS,
                (LongProbeFunction<ProcessorTasklet>) t -> t.watermarkCoalescer.coalescedWm());
        probeBuilder.register(this, "lastForwardedWm", ProbeLevel.INFO, ProbeUnit.MS,
                (LongProbeFunction<ProcessorTasklet>) t -> t.outbox.lastForwardedWm());
        probeBuilder.register(this, "lastForwardedWmLatency", ProbeLevel.INFO, ProbeUnit.MS,
                (LongProbeFunction<ProcessorTasklet>) t -> lastForwardedWmLatency());
        probeBuilder.register(this, "queuesSize", ProbeLevel.INFO, ProbeUnit.COUNT,
                (LongProbeFunction<ProcessorTasklet>) t -> t.queuesSize.get());
        probeBuilder.register(this, "queuesCapacity", ProbeLevel.INFO, ProbeUnit.COUNT,
                (LongProbeFunction<ProcessorTasklet>) t -> t.queuesCapacity.get());
    }

    private OutboxImpl createOutbox(@Nonnull OutboundCollector ssCollector) {
        OutboundCollector[] collectors = new OutboundCollector[outstreams.length + 1];
        for (int i = 0; i < outstreams.length; i++) {
            collectors[i] = outstreams[i].getCollector();
        }
        collectors[outstreams.length] = ssCollector;
        return new OutboxImpl(collectors, true, progTracker,
                serializationService, OUTBOX_BATCH_SIZE, emittedCounts);
    }

    @Override
    public void init() {
        if (serializationService.getManagedContext() != null) {
            Processor toInit = processor instanceof ProcessorWrapper
                    ? ((ProcessorWrapper) processor).getWrapped() : processor;
            Object initialized = serializationService.getManagedContext().initialize(toInit);
            assert initialized == toInit : "different object returned";
        }
        try {
            processor.init(outbox, context);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    @Override @Nonnull
    public ProgressState call() {
        assert !processorClosed : "processor closed";
        progTracker.reset();
        progTracker.notDone();
        outbox.reset();
        stateMachineStep();
        ProgressState progressState = progTracker.toProgressState();
        if (progressState.isDone()) {
            closeProcessor();
            processorClosed = true;
        }
        return progressState;
    }

    private void closeProcessor() {
        assert !processorClosed : "processor already closed";
        try {
            processor.close();
        } catch (Exception e) {
            logger.severe(jobNameAndExecutionId(context.jobConfig().getName(), context.executionId())
                    + " encountered an exception in Processor.close(), ignoring it", e);
        }
    }

    @SuppressWarnings("checkstyle:returncount")
    private void stateMachineStep() {
        switch (state) {
            case PROCESS_WATERMARK:
                if (pendingWatermark == null) {
                    long wm = watermarkCoalescer.checkWmHistory();
                    if (wm == NO_NEW_WM) {
                        state = PROCESS_INBOX;
                        stateMachineStep(); // recursion
                        break;
                    }
                    pendingWatermark = new Watermark(wm);
                }
                if (pendingWatermark.equals(IDLE_MESSAGE)
                        ? outbox.offer(IDLE_MESSAGE)
                        : processor.tryProcessWatermark(pendingWatermark)) {
                    state = PROCESS_INBOX;
                    pendingWatermark = null;
                    stateMachineStep(); // recursion
                }
                break;

            case PROCESS_INBOX:
                processInbox();
                return;

            case COMPLETE_EDGE:
                if (isSnapshotInbox()
                        ? processor.finishSnapshotRestore() : processor.completeEdge(currInstream.ordinal())) {
                    assert !outbox.hasUnfinishedItem() :
                            "outbox has unfinished item after successful completeEdge() or finishSnapshotRestore()";
                    progTracker.madeProgress();
                    state = processingState();
                }
                return;

            case SAVE_SNAPSHOT:
                if (processor.saveToSnapshot()) {
                    progTracker.madeProgress();
                    state = EMIT_BARRIER;
                }
                return;

            case EMIT_BARRIER:
                assert currentBarrier != null : "currentBarrier == null";
                if (outbox.offerToEdgesAndSnapshot(currentBarrier)) {
                    progTracker.madeProgress();
                    if (currentBarrier.isTerminal()) {
                        state = EMIT_DONE_ITEM;
                    } else {
                        currentBarrier = null;
                        receivedBarriers.clear();
                        pendingSnapshotId1++;
                        state = processingState();
                    }
                }
                return;

            case ON_SNAPSHOT_COMPLETED:
                if (processor.onSnapshotCompleted(ssContext.isLast1stPhaseSuccessful())) {
                    progTracker.madeProgress();
                    state = processingState();
                }
                return;

            case WAITING_FOR_SNAPSHOT_COMPLETED:
                long currSnapshotId2 = ssContext.activeSnapshotId2ndPhase();
                if (currSnapshotId2 >= pendingSnapshotId2) {
                    progTracker.madeProgress();
                    state = FINAL_ON_SNAPSHOT_COMPLETED;
                    stateMachineStep(); // recursion
                }
                return;

            case FINAL_ON_SNAPSHOT_COMPLETED:
                if (processor.onSnapshotCompleted(ssContext.isLast1stPhaseSuccessful())) {
                    progTracker.madeProgress();
                    state = EMIT_DONE_ITEM;
                }
                return;

            case COMPLETE:
                complete();
                return;

            case EMIT_DONE_ITEM:
                if (outbox.offerToEdgesAndSnapshot(DONE_ITEM)) {
                    state = END;
                    progTracker.done();
                }
                return;

            default:
                // note ProcessorState.END goes here
                throw new JetException("Unexpected state: " + state);
        }
    }

    private void processInbox() {
        long currSnapshotId2 = ssContext.activeSnapshotId2ndPhase();
        if (currSnapshotId2 >= pendingSnapshotId2) {
            pendingSnapshotId2 = currSnapshotId2;
            if (outbox.hasUnfinishedItem()) {
                outbox.block();
            } else {
                outbox.unblock();
                state = ON_SNAPSHOT_COMPLETED;
                progTracker.madeProgress();
                return;
            }
        }

        if (inbox.isEmpty()) {
            if (isSnapshotInbox() || processor.tryProcess()) { // TODO [viliam] tryProcess returned false, but we proceed to snapshot 2nd phase
                assert !outbox.hasUnfinishedItem() : isSnapshotInbox()
                        ? "Unfinished item before fillInbox call"
                        : "Processor.tryProcess() returned true, but there's unfinished item in the outbox";
                fillInbox();
            } else {
                return;
            }
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
            } else if (numActiveOrdinals > 0
                    && receivedBarriers.cardinality() == numActiveOrdinals) {
                // we have an empty inbox and received the current snapshot barrier from all active ordinals
                state = SAVE_SNAPSHOT;
            } else if (numActiveOrdinals == 0) {
                progTracker.madeProgress();
                state = COMPLETE;
            } else {
                state = PROCESS_WATERMARK;
            }
        }
    }

    private void complete() {
        // check ssContext to see if a barrier should be emitted
        long currSnapshotId1 = ssContext.activeSnapshotId1stPhase();
        long currSnapshotId2 = ssContext.activeSnapshotId2ndPhase();
        assert currSnapshotId1 >= pendingSnapshotId1 - 1 : "Unexpected new snapshot id: " + currSnapshotId1
                + ", expected was " + (pendingSnapshotId1 - 1) + " or more";
        assert currSnapshotId2 >= pendingSnapshotId2 - 1 : "Unexpected new snapshot id: " + currSnapshotId2
                + ", expected was " + (pendingSnapshotId2 - 1) + " or more";
        if (currSnapshotId1 >= pendingSnapshotId1) {
            pendingSnapshotId1 = currSnapshotId1;
            if (outbox.hasUnfinishedItem()) {
                outbox.block();
            } else {
                outbox.unblock();
                state = SAVE_SNAPSHOT;
                currentBarrier = new SnapshotBarrier(currSnapshotId1, ssContext.isTerminalSnapshot());
                progTracker.madeProgress();
                return;
            }
        } else if (currSnapshotId2 >= pendingSnapshotId2) {
            pendingSnapshotId2 = currSnapshotId2;
            if (outbox.hasUnfinishedItem()) {
                outbox.block();
            } else {
                outbox.unblock();
                state = ON_SNAPSHOT_COMPLETED;
                progTracker.madeProgress();
                return;
            }
        }
        if (processor.complete()) {
            progTracker.madeProgress();
            state = pendingSnapshotId2 < pendingSnapshotId1
                    ? WAITING_FOR_SNAPSHOT_COMPLETED
                    : EMIT_DONE_ITEM;
        }
    }

    private void fillInbox() {
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
            if (waitForAllBarriers && receivedBarriers.get(currInstream.ordinal())) {
                instreamCursor.advance();
                continue;
            }
            result = currInstream.drainTo(addToInboxFunction);
            progTracker.madeProgress(result.isMadeProgress());

            // check if the last drained item is special
            Object lastItem = inbox.queue().peekLast();
            if (lastItem instanceof Watermark) {
                long newWmValue = ((Watermark) inbox.queue().removeLast()).timestamp();
                long wm = watermarkCoalescer.observeWm(currInstream.ordinal(), newWmValue);
                if (wm != NO_NEW_WM) {
                    pendingWatermark = new Watermark(wm);
                }
            } else if (lastItem instanceof SnapshotBarrier) {
                SnapshotBarrier barrier = (SnapshotBarrier) inbox.queue().removeLast();
                observeBarrier(currInstream.ordinal(), barrier);
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
                break;
            }
        } while (!result.isMadeProgress() && instreamCursor.value() != first);

        // we are the only updating thread, no need for CAS operations
        lazyAdd(receivedCounts, currInstream.ordinal(), inbox.size());
        if (!inbox.isEmpty()) {
            lazyIncrement(receivedBatches, currInstream.ordinal());
        }
        queuesCapacity.lazySet(instreamCursor == null ? 0 : sum(instreamCursor.getList(), InboundEdgeStream::capacities));
        queuesSize.lazySet(instreamCursor == null ? 0 : sum(instreamCursor.getList(), InboundEdgeStream::sizes));
    }

    private CircularListCursor<InboundEdgeStream> popInstreamGroup() {
        return Optional.ofNullable(instreamGroupQueue.poll())
                       .map(CircularListCursor::new)
                       .orElse(null);
    }

    @Override
    public String toString() {
        String jobPrefix = context.jobConfig().getName() == null ? "" : context.jobConfig().getName() + "/";
        return "ProcessorTasklet{" + jobPrefix + context.vertexName() + '#' + context.globalProcessorIndex() + '}';
    }

    private void observeBarrier(int ordinal, SnapshotBarrier barrier) {
        if (barrier.snapshotId() < pendingSnapshotId1) {
            throw new JetException("Unexpected snapshot barrier ID " + barrier.snapshotId() + " from ordinal " + ordinal +
                    " expected " + pendingSnapshotId1);
        }
        currentBarrier = barrier;
        if (barrier.isTerminal()) {
            // Switch to exactly-once mode. The reason is that there will be DONE_ITEM just after the
            // terminal barrier and if we process it before receiving the other barriers, it could cause
            // the watermark to advance. The exactly-once mode disallows processing of any items after
            // the barrier before the barrier is processed.
            waitForAllBarriers = true;
        }
        receivedBarriers.set(ordinal);
    }

    /**
     * Initial state of the processor. If there are no inbound ordinals left,
     * we will go to COMPLETE state otherwise to PROCESS_INBOX or
     * PROCESS_WATERMARK.
     */
    private ProcessorState processingState() {
        return pendingWatermark != null ? PROCESS_WATERMARK
                : instreamCursor == null ? COMPLETE : PROCESS_INBOX;
    }

    /**
     * Returns, if the inbox we are currently on is the snapshot restoring inbox.
     */
    private boolean isSnapshotInbox() {
        return currInstream != null && currInstream.priority() == Integer.MIN_VALUE;
    }

    private long lastForwardedWmLatency() {
        long wm = outbox.lastForwardedWm();
        if (wm == IDLE_MESSAGE.timestamp()) {
            return Long.MIN_VALUE; // idle
        }
        if (wm == Long.MIN_VALUE) {
            return Long.MAX_VALUE; // no wms emitted
        }
        return System.currentTimeMillis() - wm;
    }

    @Override
    public boolean isCooperative() {
        return processor.isCooperative();
    }

    @Override
    public void close() {
        if (!processorClosed) {
            closeProcessor();
            processorClosed = true;
        }
    }

    // visible for test
    ProcessorState state() {
        return state;
    }
}
