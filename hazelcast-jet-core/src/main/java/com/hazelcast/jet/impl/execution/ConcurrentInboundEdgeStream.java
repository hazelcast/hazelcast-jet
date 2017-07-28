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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.jet.impl.util.SkewReductionPolicy;

import java.util.function.Consumer;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;

/**
 * {@link InboundEdgeStream} implemented in terms of a {@link ConcurrentConveyor}.
 * The conveyor has as many 1-to-1 concurrent queues as there are upstream tasklets
 * contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker = new ProgressTracker();
    private long lastEmittedWm = Long.MIN_VALUE;
    private final boolean blockOnBarrier;

    private final SkewReductionPolicy skewReductionPolicy;
    private final BarrierWatcher barrierWatcher;
    private SnapshotBarrier barrierToDrain;
    // TODO initialize to snapshot we are restoring from
    private long barrierAt;

    /**
     * @param blockOnBarrier If true, queues won't be drained until the same
     *     barrier is received from all of them. This will enforce exactly once
     *     vs. at least once, if it is false.
     */
    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority,
                                       boolean blockOnBarrier) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.blockOnBarrier = blockOnBarrier;

        skewReductionPolicy = new SkewReductionPolicy(conveyor.queueCount());
        barrierWatcher = new BarrierWatcher(conveyor.queueCount());
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    /**
     * Drains multiple inbound queues into the {@code dest} collection.
     *
     * <p>Following rules apply:<ul>
     *
     * <li>{@link Watermark}s are handled specially: {@link SkewReductionPolicy}
     * might decide to temporarily stop draining some queues, if they are
     * ahead, or ignore watermark from some other, if they are too much behind.
     * Generally, watermark is added to {@code dest} after it is received from
     * all queues.
     *
     * <li>{@link SnapshotBarrier}s are handled specially, as directed by
     * {@link BarrierWatcher}. Same barrier has to be received from all queues
     * to be added to the {@code dest} collection. Queues which already had the
     * barrier are not drained until all other have it, if {@link
     * #blockOnBarrier} is true.
     *
     * <li>Furthermore, {@link SnapshotBarrier} is always added to {@code dest}
     * as the only item in a separate call to this method.
     * </ul>
     */
    @Override
    public ProgressState drainTo(Consumer<Object> dest) {
        if (barrierToDrain != null && barrierToDrain.snapshotId() < Long.MAX_VALUE) {
            dest.accept(barrierToDrain);
            barrierAt = barrierToDrain.snapshotId();
            barrierToDrain = null;
            return ProgressState.MADE_PROGRESS;
        }

        tracker.reset();
        for (int drainOrderIdx = 0; drainOrderIdx < conveyor.queueCount(); drainOrderIdx++) {
            int queueIndex = skewReductionPolicy.toQueueIndex(drainOrderIdx);
            final QueuedPipe<Object> q = conveyor.queue(queueIndex);
            if (q == null) {
                continue;
            }
            boolean done = false;
            if (blockOnBarrier && barrierWatcher.isBlocked(queueIndex)) {
                continue;
            }

            for (Object item; (item = q.poll()) != null; ) {
                tracker.madeProgress();
                if (item == DONE_ITEM) {
                    done = true;
                    conveyor.removeQueue(queueIndex);
                    long newBarrier = barrierWatcher.markQueueDone(queueIndex);
                    if (newBarrier > barrierAt) {
                        barrierToDrain = new SnapshotBarrier(newBarrier);
                        // stop now, barrier will be added as the sole item in next call
                        return ProgressState.MADE_PROGRESS;
                    }
                    break;
                }
                if (item instanceof Watermark) {
                    skewReductionPolicy.observeWm(queueIndex, ((Watermark) item).timestamp());
                } else if (item instanceof SnapshotBarrier) {
                    if (barrierWatcher.observe(queueIndex, ((SnapshotBarrier) item).snapshotId())) {
                        barrierToDrain = (SnapshotBarrier) item;
                        // stop now, barrier will be added as the sole item in next call
                        return ProgressState.MADE_PROGRESS;
                    }
                } else {
                    dest.accept(item);
                }
            }

            if (!done) {
                tracker.notDone();
            }
        }

        long bottomWm = skewReductionPolicy.bottomObservedWm();
        if (bottomWm > lastEmittedWm) {
            dest.accept(new Watermark(bottomWm));
            lastEmittedWm = bottomWm;
        }

        return tracker.toProgressState();
    }
}
