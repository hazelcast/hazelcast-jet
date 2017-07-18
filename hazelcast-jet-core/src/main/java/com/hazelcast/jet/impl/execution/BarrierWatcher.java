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

public class BarrierWatcher {

    private final long[] barrierAt;

    public BarrierWatcher(int queueCount) {
        barrierAt = new long[queueCount];
    }

    /**
     * Marks the queue as completed (after receiving {@link
     * DoneItem#DONE_ITEM}.
     *
     * @return barrier at which all queues currently are
     */
    public long markQueueDone(int queueIndex) {
        barrierAt[queueIndex] = Long.MAX_VALUE;

        // if this queue was the minimum queue and it was the only queue at this barrier, we'll forward it
        long min = Long.MAX_VALUE;
        for (long b : barrierAt) {
            if (min > b) {
                min = b;
            }
        }

        return min;
    }

    /**
     * @return true, iff the barrier can now be forwarded (has been received from all queues)
     */
    public boolean observe(int queueIndex, long newBarrier) {
        assert barrierAt[queueIndex] < newBarrier : "SnapshotId not monotonically increasing. Before: "
                + barrierAt[queueIndex] + ", now: " + newBarrier;

        barrierAt[queueIndex] = newBarrier;

        return !isBlocked(queueIndex);
    }

    /**
     * @return true, iff the queue at {@code queueIndex} should be blocked, because it already had
     * the barrier we are expecting from all other queues.
     */
    public boolean isBlocked(int queueIndex) {
        if (barrierAt[queueIndex] == Long.MAX_VALUE) {
            return false;
        }
        for (long b : barrierAt) {
            if (b < barrierAt[queueIndex]) {
                return true;
            }
        }
        return false;
    }
}
