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
import com.hazelcast.jet.SnapshotStorage;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.nio.serialization.Data;
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

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public abstract class ProcessorTaskletBase implements Tasklet {

    final ProgressTracker progTracker = new ProgressTracker();
    final Processor processor;
    final OutboundEdgeStream[] outstreams;
    InboundEdgeStream currInstream;
    Outbox outbox;

    private final ProcCtx context;
    private final ArrayDequeInbox inbox = new ArrayDequeInbox(progTracker);
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private final SnapshotState snapshotState;
    private final Queue<Object> snapshotQueue;
    private final SerializationService serializationService;
    private final SnapshotStorage snapshotStorage;
    private long completedSnapshotId;
    private long requestedSnapshotId;

    private ProcessorState state = ProcessorState.STATE_NULLARY_PROCESS;

    ProcessorTaskletBase(ProcCtx context,
                         Processor processor,
                         List<InboundEdgeStream> instreams,
                         List<OutboundEdgeStream> outstreams,
                         SnapshotState snapshotState,
                         Queue<Object> snapshotQueue,
                         SerializationService serializationService) {
        Preconditions.checkNotNull(processor, "processor");
        this.context = context;
        this.processor = processor;
        this.instreamGroupQueue = instreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new, toCollection(ArrayList::new)))
                .entrySet().stream()
                .map(Entry::getValue)
                .collect(toCollection(ArrayDeque::new));
        this.outstreams = outstreams.stream()
                                    .sorted(comparing(OutboundEdgeStream::ordinal))
                                    .toArray(OutboundEdgeStream[]::new);
        this.snapshotState = snapshotState;
        this.snapshotQueue = snapshotQueue;
        this.serializationService = serializationService;

        instreamCursor = popInstreamGroup();
        completedSnapshotId = snapshotState != null ? snapshotState.getCurrentSnapshotId() : Long.MAX_VALUE;

        // TODO blocking snapshotStorage for blocking processor, that will never return false from offer
        if (snapshotQueue == null) {
            snapshotStorage = null;
        } else {
            snapshotStorage = new SnapshotStorage() {
                private Entry<Data, Data> pendingEntry;

                @Override
                public boolean offer(Object key, Object value) {
                    if (pendingEntry != null) {
                        if (!snapshotQueue.offer(pendingEntry)) {
                            return false;
                        }
                    }

                    // We serialize the key and value immediately to effectively clone them,
                    // so the caller can modify them right after they are accepted by this method.
                    // TODO use Map's partitioning strategy
                    Data sKey = ProcessorTaskletBase.this.serializationService.toData(key);
                    Data sValue = ProcessorTaskletBase.this.serializationService.toData(value);
                    pendingEntry = entry(sKey, sValue);

                    boolean success = snapshotQueue.offer(pendingEntry);
                    if (success) {
                        pendingEntry = null;
                    }
                    return success;
                }
            };
        }
    }

    @Override @Nonnull
    public ProgressState call() {
        progTracker.reset();

        if (state == ProcessorState.STATE_START_SNAPSHOT) {
            if (instreamCursor == null) {
                // If our processor is now a source, check the flag in ExecutionContext to start a snapshot.
                // Any processor becomes a source after its input completes.
                requestedSnapshotId = snapshotState.getCurrentSnapshotId();
                assert requestedSnapshotId >= completedSnapshotId;
            }
            if (requestedSnapshotId == completedSnapshotId) {
                // No new snapshot requested, skip snapshot creation
                state = ProcessorState.STATE_NULLARY_PROCESS;
            } else if (snapshotQueue == null) {
                // New snapshot requested, but our processor is stateless. Just forward the barrier.
                state = ProcessorState.STATE_SNAPSHOT_BARRIER_TO_OUTBOX;
            } else if (snapshotQueue.offer(new SnapshotStartBarrier(requestedSnapshotId))) {
                state = ProcessorState.STATE_DO_SNAPSHOT;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ProcessorState.STATE_DO_SNAPSHOT) {
            if (processor.saveSnapshot(snapshotStorage)) {
                state = ProcessorState.STATE_SNAPSHOT_BARRIER_TO_SNAPSHOT_QUEUE;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ProcessorState.STATE_SNAPSHOT_BARRIER_TO_SNAPSHOT_QUEUE) {
            if (snapshotQueue.offer(new SnapshotBarrier(requestedSnapshotId))) {
                state = ProcessorState.STATE_SNAPSHOT_BARRIER_TO_OUTBOX;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ProcessorState.STATE_SNAPSHOT_BARRIER_TO_OUTBOX) {
            if (outbox.offer(new SnapshotBarrier(requestedSnapshotId))) {
                completedSnapshotId = requestedSnapshotId;
                state = ProcessorState.STATE_NULLARY_PROCESS;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ProcessorState.STATE_NULLARY_PROCESS) {
            if (processor.tryProcess()) {
                state = ProcessorState.STATE_PROCESS_OR_COMPLETE;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ProcessorState.STATE_PROCESS_OR_COMPLETE) {
            if (inbox.isEmpty()) {
                tryFillInbox();
                // Barrier is put to the inbox as the sole item by ConcurrentInboundEdgeStream. In
                // this case, proceed to process the snapshot
                if (inbox.size() == 1 && inbox.peek() instanceof SnapshotBarrier) {
                    SnapshotBarrier barrier = (SnapshotBarrier) inbox.remove();
                    requestedSnapshotId = barrier.snapshotId;
                    state = snapshotQueue == null
                            ? ProcessorState.STATE_SNAPSHOT_BARRIER_TO_OUTBOX
                            : ProcessorState.STATE_START_SNAPSHOT;
                    return call(); // recursive call
                }
            } else {
                progTracker.notDone();
            }

            if (progTracker.isDone()) {
                if (processor.complete()) {
                    state = ProcessorState.STATE_ADD_DONE_ITEM_OUTBOX;
                } else {
                    state = ProcessorState.STATE_START_SNAPSHOT;
                    progTracker.notDone();
                }
            } else {
                if (!inbox().isEmpty()) {
                    processor.process(currInstream.ordinal(), inbox());
                }
                if (inbox.isEmpty()) {
                    state = ProcessorState.STATE_START_SNAPSHOT;
                }
            }
        }

        if (state == ProcessorState.STATE_ADD_DONE_ITEM_OUTBOX) {
            if (outbox.offer(DONE_ITEM)) {
                state = ProcessorState.STATE_ADD_DONE_ITEM_SNAPSHOT;
            } else {
                progTracker.notDone();
            }
        }

        if (state == ProcessorState.STATE_ADD_DONE_ITEM_SNAPSHOT) {
            if (snapshotQueue == null || snapshotQueue.offer(DONE_ITEM)) {
                state = ProcessorState.STATE_PROCESSOR_COMPLETED;
            } else {
                progTracker.notDone();
            }
        }

        // most steps can add items to outbox, let's flush them
        tryFlushOutbox();
        return progTracker.toProgressState();
    }

    Inbox inbox() {
        return inbox;
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        context.initJobFuture(jobFuture);
        processor.init(outbox, context);
    }

    void tryFillInbox() {
        if (instreamCursor == null) {
            return;
        }
        progTracker.notDone();
        final InboundEdgeStream first = instreamCursor.value();
        ProgressState result;
        do {
            currInstream = instreamCursor.value();
            result = currInstream.drainTo(inbox::add);
            progTracker.madeProgress(result.isMadeProgress());
            if (result.isDone()) {
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

    private enum ProcessorState {
        /**
         * Check, if new snapshot is requested and wait to accept the {@link
         * SnapshotStartBarrier} by the queue.
         */
        STATE_START_SNAPSHOT,

        /**
         * Doing calls to {@link Processor#saveSnapshot(SnapshotStorage)} until it returns true
         */
        STATE_DO_SNAPSHOT,

        /**
         * Waiting to accept the {@link SnapshotBarrier} by the queue
         */
        STATE_SNAPSHOT_BARRIER_TO_OUTBOX,

        /**
         * Waiting to accept the {@link SnapshotBarrier} by the {@link #snapshotQueue}
         */
        STATE_SNAPSHOT_BARRIER_TO_SNAPSHOT_QUEUE,

        /**
         * Doing calls to {@link Processor#tryProcess()} until it returns true
         */
        STATE_NULLARY_PROCESS,

        /**
         * Doing calls to {@link Processor#process(int, Inbox)} until the inbox is empty or to {@link Processor#complete()} until it returns true
         */
        STATE_PROCESS_OR_COMPLETE,

        /**
         * Waiting until outbox accepts DONE_ITEM
         */
        STATE_ADD_DONE_ITEM_OUTBOX,

        /**
         * Waiting until snapshot storage accepts DONE_ITEM
         */
        STATE_ADD_DONE_ITEM_SNAPSHOT,

        /**
         * waiting to flush the outbox. This is a terminal state
         */
        STATE_PROCESSOR_COMPLETED
    }
}

