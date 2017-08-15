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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Snapshottable;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.OutboxImpl;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_BARRIER;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.END;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_INBOX;
import static com.hazelcast.jet.impl.execution.ProcessorState.SAVE_SNAPSHOT;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public abstract class ProcessorTaskletBase implements Tasklet {

    private final ProgressTracker progTracker = new ProgressTracker();
    private final OutboundEdgeStream[] outstreams;
    private final OutboxImpl outbox;
    private final ProcCtx context;

    private final Processor processor;

    // casted #processor to Snapshottable or null, if #processor does not implement it
    private final Snapshottable snapshottable;
    private final SnapshotContext snapshotContext;
    private final BitSet barrierReceived; // indicates if current snapshot is received on the ordinal

    private final ArrayDequeInbox inbox = new ArrayDequeInbox(progTracker);
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;

    private int activeOrdinals; // counter for remaining active ordinals
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private InboundEdgeStream currInstream;
    private ProcessorState state = PROCESS_INBOX;
    private long currSnapshot;


    ProcessorTaskletBase(@Nonnull ProcCtx context,
                         @Nonnull Processor processor,
                         @Nonnull List<InboundEdgeStream> instreams,
                         @Nonnull List<OutboundEdgeStream> outstreams,
                         @Nullable SnapshotContext snapshotContext,
                         @Nullable Queue<Object> snapshotQueue) {
        Preconditions.checkNotNull(processor, "processor");
        this.context = context;
        this.processor = processor;
        this.snapshottable = processor instanceof Snapshottable ? (Snapshottable) processor : null;
        this.activeOrdinals = instreams.size();
        this.instreamGroupQueue = instreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new, toCollection(ArrayList::new)))
                .entrySet().stream()
                .map(Entry::getValue)
                .collect(toCollection(ArrayDeque::new));
        this.outstreams = outstreams.stream()
                                    .sorted(comparing(OutboundEdgeStream::ordinal))
                                    .toArray(OutboundEdgeStream[]::new);
        this.snapshotContext = snapshotContext;

        instreamCursor = popInstreamGroup();
        outbox = createOutbox(snapshotQueue);
        barrierReceived = new BitSet(instreams.size());
        state = initialState();
    }

    private OutboxImpl createOutbox(Queue<Object> snapshotQueue) {
        Function<Object, ProgressState>[] functions = new Function[outstreams.length + (snapshotQueue == null ? 0 : 1)];
        for (int i = 0; i < outstreams.length; i++) {
            OutboundCollector collector = outstreams[i].getCollector();
            functions[i] = item -> item instanceof Watermark || item instanceof SnapshotBarrier || item == DONE_ITEM
                    ? collector.offerBroadcast(item) : collector.offer(item);
        }
        if (snapshotQueue != null) {
            functions[outstreams.length] = e -> snapshotQueue.offer(e) ? DONE : NO_PROGRESS;
        }

        return createOutboxInt(functions, snapshotQueue != null, progTracker,
                context.getSerializationService());
    }

    protected abstract OutboxImpl createOutboxInt(Function<Object, ProgressState>[] outstreams, boolean hasSnapshot,
                                                  ProgressTracker progTracker, SerializationService serializationService);

    private ProcessorState initialState() {
        return instreamCursor == null ? COMPLETE : PROCESS_INBOX;
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        context.initJobFuture(jobFuture);
        processor.init(outbox, context);
    }

    @Override @Nonnull
    public ProgressState call() {
        progTracker.reset();

        switch (state) {
            case PROCESS_INBOX:
                progTracker.notDone();
                // call nullary process
                if (inbox.isEmpty() && processor.tryProcess()) {
                    fillInbox();
                }
                if (!inbox.isEmpty()) {
                    processor.process(currInstream.ordinal(), inbox);
                    // we have emptied the inbox and received the current snapshot from all active ordinals
                    if (inbox.isEmpty()
                            && instreamGroupQueue.size() > 0
                            && barrierReceived.cardinality() == activeOrdinals) {
                        // if processor does not support snapshotting, we will skip SAVE_SNAPSHOT state
                        state = snapshottable == null ? EMIT_BARRIER : SAVE_SNAPSHOT;
                        break;
                    }
                }
                if (inbox.isEmpty() && instreamCursor == null) {
                    state = COMPLETE;
                }
                break;

            case SAVE_SNAPSHOT:
                progTracker.notDone();
                if (snapshottable.saveSnapshot()) {
                    state = EMIT_BARRIER;
                    break;
                }
                break;

            case EMIT_BARRIER:
                progTracker.notDone();
                if (outbox.offerToEdgesAndSnapshot(new SnapshotBarrier(currSnapshot))) {
                    barrierReceived.clear();
                    state = initialState();
                    break;
                }
                break;

            case COMPLETE:
                progTracker.notDone();
                // check snapshotContext to see if a new barrier should be emitted
                if (snapshotContext != null) {
                    long newSnapshotId = snapshotContext.getCurrentSnapshotId();
                    if (newSnapshotId > currSnapshot) {
                        assert newSnapshotId == currSnapshot + 1 : "Unexpected new snapshot id " + newSnapshotId
                                + ", current was" + currSnapshot;
                        currSnapshot = newSnapshotId;
                        state = EMIT_BARRIER;
                        break;
                    }
                }
                if (processor.complete()) {
                    progTracker.madeProgress();
                    state = EMIT_DONE_ITEM;
                }
                break;

            case EMIT_DONE_ITEM:
                if (!outbox.offerToEdgesAndSnapshot(DONE_ITEM)) {
                    progTracker.notDone();
                    break;
                }
                state = END;
                break;

            default:
                // note ProcessorState.END goes here
                throw new JetException("Unexpected state: " + state);
        }
        return progTracker.toProgressState();
    }

    private void fillInbox() {
        if (instreamCursor == null) {
            return;
        }
        final InboundEdgeStream first = instreamCursor.value();
        ProgressState result;
        do {
            currInstream = instreamCursor.value();
            result = NO_PROGRESS;

            // skip ordinals where a snapshot barrier has already been received
            if (snapshotContext != null && snapshotContext.getGuarantee() == ProcessingGuarantee.EXACTLY_ONCE
                    && barrierReceived.get(currInstream.ordinal())) {
                continue;
            }
            result = currInstream.drainTo(inbox::add);
            progTracker.madeProgress(result.isMadeProgress());

            if (result.isDone()) {
                barrierReceived.clear(currInstream.ordinal());
                instreamCursor.remove();
                activeOrdinals--;
            }

            // check if last item was snapshot
            if (inbox.peekLast() instanceof SnapshotBarrier) {
                SnapshotBarrier barrier = (SnapshotBarrier) inbox.removeLast();
                observeSnapshot(currInstream.ordinal(), barrier.snapshotId());
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

    protected OutboxImpl getOutbox() {
        return outbox;
    }

    @Override
    public String toString() {
        return "ProcessorTasklet{vertex=" + context.vertexName() + ", processor=" + processor + '}';
    }

    private void observeSnapshot(int ordinal, long snapshotId) {
        if (snapshotId != currSnapshot) {
            throw new JetException("Unexpected snapshot barrier " + snapshotId + " from ordinal " + ordinal +
                    " expected " + currSnapshot);
        }
        barrierReceived.set(ordinal);
    }

}

