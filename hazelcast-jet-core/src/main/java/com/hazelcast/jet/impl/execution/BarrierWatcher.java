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

import java.util.BitSet;

class BarrierWatcher {

    private final BitSet barrierReceived;
    private int numAwaiting;
    private int numQueuesNotDone;
    private long currentSnapshotId;

    BarrierWatcher(int queueCount) {
        barrierReceived = new BitSet(queueCount);
        numQueuesNotDone = queueCount;
        numAwaiting = numQueuesNotDone;
    }

    /**
     * Marks the queue as completed (after receiving {@link
     * DoneItem#DONE_ITEM}.
     *
     * @return snapshotId received from all other queues which can now be
     * forwarded or 0, if some queues still expect barrier.
     */
    long markQueueDone(int queueIndex) {
        assert numQueuesNotDone > 0;
        numQueuesNotDone--;
        long previousSnapshotId = currentSnapshotId;
        if (previousSnapshotId != 0) {
            if (observe(queueIndex, currentSnapshotId)) {
                return previousSnapshotId;
            }
        } else {
            numAwaiting--;
        }
        return 0;
    }

    /**
     * @return true, iff the barrier can now be forwarded (has been received from all queues)
     */
    boolean observe(int queueIndex, long newBarrier) {
        assert currentSnapshotId == 0 || currentSnapshotId == newBarrier
                : "Different barrier received from queues: expected: " + currentSnapshotId + ", got: " + newBarrier;

        assert !barrierReceived.get(queueIndex) : "Barrier already received on queue";

        assert newBarrier != 0 : "snapshotId=0 is not allowed";

        currentSnapshotId = newBarrier;

        barrierReceived.set(queueIndex);
        numAwaiting--;

        if (numAwaiting == 0) {
            barrierReceived.clear();
            numAwaiting = numQueuesNotDone;
            currentSnapshotId = 0;
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return true, iff the queue at {@code queueIndex} should be blocked, because it already had
     * the barrier we are expecting from all other queues.
     */
    boolean isBlocked(int queueIndex) {
        return barrierReceived.get(queueIndex);
    }
}
