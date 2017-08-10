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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;

import java.util.Arrays;
import java.util.BitSet;
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
    private final boolean waitForSnapshot;
    private final long[] queueWms;

    private final BitSet barrierReceived; // indicates if current snapshot is received on the queue

    private long currSnapshot = 0; // current expected snapshot
    private long lastEmittedWm = Long.MIN_VALUE;

    private long activeQueues; // number of active queues remaining

    /**
     * @param waitForSnapshot If true, queues won't be drained until the same
     *                        barrier is received from all of them. This will enforce exactly once
     *                        vs. at least once, if it is false.
     */
    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority,
                                       boolean waitForSnapshot) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.waitForSnapshot = waitForSnapshot;

        queueWms = new long[conveyor.queueCount()];
        Arrays.fill(queueWms, Long.MIN_VALUE);

        activeQueues = conveyor.queueCount();
        barrierReceived = new BitSet(conveyor.queueCount());
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    @Override
    public ProgressState drainTo(Consumer<Object> dest) {
        tracker.reset();
        for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
            final QueuedPipe<Object> q = conveyor.queue(queueIndex);
            if (q == null) {
                continue;
            }

            // skip queues where a snapshot barrier has already been received
            if (waitForSnapshot && barrierReceived.get(queueIndex)) {
                continue;
            }

            for (Object item; (item = q.poll()) != null; ) {
                tracker.madeProgress();
                if (item == DONE_ITEM) {
                    conveyor.removeQueue(queueIndex);
                    activeQueues--;
                    // we are done with this queue
                    break;
                }
                if (item instanceof Watermark) {
                    // do not drain more items from this queue after observing a WM
                    observeWm(queueIndex, ((Watermark) item).timestamp());
                    break;
                }
                if (item instanceof SnapshotBarrier) {
                    SnapshotBarrier barrier = (SnapshotBarrier) item;
                    if (barrier.snapshotId() != currSnapshot) {
                        throw new JetException("Unexpected snapshot barrier "
                                + barrier.snapshotId() + ", expected " + currSnapshot);
                    }
                    barrierReceived.set(queueIndex);
                    // do not drain more items from this queue after snapshot barrier
                    break;
                }

                // forward everything else
                dest.accept(item);
            }
        }

        if (activeQueues == 0) {
            return tracker.toProgressState();
        }

        tracker.notDone();

        // coalesce WMs received and emit new WM if needed
        long bottomWm = bottomObservedWm();
        if (bottomWm > lastEmittedWm) {
            lastEmittedWm = bottomWm;
            dest.accept(new Watermark(bottomWm));
        }

        // if we have received the current snapshot from all active queues, forward it
        if (barrierReceived.cardinality() == activeQueues) {
            dest.accept(new SnapshotBarrier(currSnapshot));
            currSnapshot++;
            barrierReceived.clear();
        }
        return tracker.toProgressState();
    }


    private void observeWm(int queueIndex, final long wmValue) {
        if (queueWms[queueIndex] >= wmValue) {
            throw new JetException("Watermarks not monotonically increasing on queue: " +
                    "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
        }
        queueWms[queueIndex] = wmValue;
    }

    private long bottomObservedWm() {
        long min = queueWms[0];
        for (int i = 1; i < queueWms.length; i++) {
            if (queueWms[i] < min) {
                min = queueWms[i];
            }
        }
        return min;
    }
}
