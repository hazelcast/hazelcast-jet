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
import com.hazelcast.jet.SnapshotStorage;
import com.hazelcast.jet.Snapshottable;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.*;
import static com.hazelcast.jet.impl.util.ProgressState.*;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public abstract class ProcessorTaskletBase implements Tasklet {

    final ProgressTracker progTracker = new ProgressTracker();
    final OutboundEdgeStream[] outstreams;
    Outbox outbox;
    private final Processor processor;
    // casted #processor to Snapshottable or null, if #processor does not implement it
    private final Snapshottable snapshottable;
    private InboundEdgeStream currInstream;
    private final ProcessingGuarantee processingGuarantee;

    private final ProcCtx context;
    private final ArrayDequeInbox inbox = new ArrayDequeInbox(progTracker);
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private final SnapshotContext snapshotContext;
    private final Queue<Object> snapshotQueue;
    private final SnapshotStorage snapshotStorage;
    private long completedSnapshotId;
    private long requestedSnapshotId;
    private BarrierWatcher barrierWatcher;

    private ProcessorState state = NULLARY_PROCESS;
    private long pendingBarrier;

    ProcessorTaskletBase(ProcCtx context,
                         Processor processor,
                         List<InboundEdgeStream> instreams,
                         List<OutboundEdgeStream> outstreams,
                         SnapshotContext snapshotContext,
                         Queue<Object> snapshotQueue,
                         SerializationService serializationService, ProcessingGuarantee processingGuarantee) {
        Preconditions.checkNotNull(processor, "processor");
        this.context = context;
        this.processor = processor;
        this.snapshottable = processor instanceof Snapshottable ? (Snapshottable) processor : null;
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
        this.snapshotQueue = snapshotQueue;
        this.processingGuarantee = processingGuarantee;

        instreamCursor = popInstreamGroup();
        completedSnapshotId = snapshotContext != null ? snapshotContext.getCurrentSnapshotId() : Long.MAX_VALUE;
        snapshotStorage = snapshotQueue == null ? null : createSnapshotStorage(snapshotQueue, serializationService);
        barrierWatcher = new BarrierWatcher(instreams.size());
    }

    protected abstract SnapshotStorageImpl createSnapshotStorage(Queue<Object> snapshotQueue,
                                                                 SerializationService serializationService);

    @Override @Nonnull
    public ProgressState call() {
        progTracker.reset();

        if (state == START_SNAPSHOT) {
            if (instreamCursor == null) {
                // If our processor is now a source, check the flag in ExecutionContext to start a snapshot.
                // Any processor becomes a source after its input completes.
                requestedSnapshotId = snapshotContext.getCurrentSnapshotId();
                assert requestedSnapshotId >= completedSnapshotId;
            }
            if (requestedSnapshotId == completedSnapshotId) {
                // No new snapshot requested, skip snapshot creation
                state = NULLARY_PROCESS;
            } else if (snapshotQueue == null) {
                // New snapshot requested, but our processor is stateless. Just forward the barrier.
                state = SNAPSHOT_BARRIER_TO_OUTBOX;
            } else if (snapshotQueue.offer(new SnapshotStartBarrier(requestedSnapshotId))) {
                state = DO_SNAPSHOT;
            } else {
                progTracker.notDone();
            }
        }

        if (state == DO_SNAPSHOT) {
            if (snapshottable.saveSnapshot(snapshotStorage)) {
                state = SNAPSHOT_BARRIER_TO_SNAPSHOT_QUEUE;
            } else {
                progTracker.notDone();
            }
        }

        if (state == SNAPSHOT_BARRIER_TO_SNAPSHOT_QUEUE) {
            if (snapshotQueue.offer(new SnapshotBarrier(requestedSnapshotId))) {
                state = SNAPSHOT_BARRIER_TO_OUTBOX;
            } else {
                progTracker.notDone();
            }
        }

        if (state == SNAPSHOT_BARRIER_TO_OUTBOX) {
            if (outbox.offer(new SnapshotBarrier(requestedSnapshotId))) {
                completedSnapshotId = requestedSnapshotId;
                state = NULLARY_PROCESS;
            } else {
                progTracker.notDone();
            }
        }

        if (state == NULLARY_PROCESS) {
            if (processor.tryProcess()) {
                state = PROCESS_OR_COMPLETE;
            } else {
                progTracker.notDone();
            }
        }

        if (state == PROCESS_OR_COMPLETE) {
            if (inbox.isEmpty()) {
                tryFillInbox();
                // Barrier is put to the inbox as the sole item by ConcurrentInboundEdgeStream. In
                // this case, proceed to process the snapshot
                if (inbox.size() == 1 && inbox.peek() instanceof SnapshotBarrier) {
                    SnapshotBarrier barrier = (SnapshotBarrier) inbox.remove();
                    if (barrierWatcher.observe(currInstream.ordinal(), requestedSnapshotId)) {
                        requestedSnapshotId = barrier.snapshotId();
                        state = snapshotQueue == null
                                ? SNAPSHOT_BARRIER_TO_OUTBOX
                                : START_SNAPSHOT;
                        return call(); // recursive call
                    }
                }
            } else {
                progTracker.notDone();
            }

            if (progTracker.isDone()) {
                if (processor.complete()) {
                    state = ADD_DONE_ITEM_OUTBOX;
                } else {
                    state = START_SNAPSHOT;
                    progTracker.notDone();
                }
            } else {
                if (!inbox.isEmpty()) {
                    processor.process(currInstream.ordinal(), inbox);
                }
                if (inbox.isEmpty()) {
                    state = START_SNAPSHOT;
                }
            }
        }

        if (state == ADD_DONE_ITEM_OUTBOX) {
            if (outbox.offer(DONE_ITEM)) {
                state = ADD_DONE_ITEM_SNAPSHOT;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ADD_DONE_ITEM_SNAPSHOT) {
            if (snapshotQueue == null || snapshotQueue.offer(DONE_ITEM)) {
                state = PROCESSOR_COMPLETED;
            } else {
                progTracker.notDone();
            }
        }

        // most steps can add items to outbox, let's flush them
        tryFlushOutbox();
        return progTracker.toProgressState();
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        context.initJobFuture(jobFuture);
        processor.init(outbox, context);
    }

    private void tryFillInbox() {
        if (instreamCursor == null) {
            return;
        }
        progTracker.notDone();
        final InboundEdgeStream first = instreamCursor.value();
        ProgressState result;
        do {
            if (pendingBarrier != 0) {
                inbox.add(new SnapshotBarrier(pendingBarrier));
                return;
            }
            currInstream = instreamCursor.value();
            result = NO_PROGRESS;
            if (processingGuarantee == ProcessingGuarantee.EXACTLY_ONCE
                    && barrierWatcher.isBlocked(currInstream.ordinal())) {
                continue;
            }
            result = currInstream.drainTo(inbox::add);
            progTracker.madeProgress(result.isMadeProgress());
            if (result.isDone()) {
                pendingBarrier = barrierWatcher.markQueueDone(currInstream.ordinal());
                instreamCursor.remove();
            }
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

    public boolean isSink() {
        return outstreams.length == 0;
    }

    @Override
    public String toString() {
        return "ProcessorTasklet{vertex=" + context.vertexName() + ", processor=" + processor + '}';
    }

    protected abstract void tryFlushOutbox();

}

