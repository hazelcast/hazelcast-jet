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

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.operation.SnapshotOperation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SnapshotContext {

    private static final int NO_SNAPSHOT = -1;

    /**
     * SnapshotId of snapshot currently being created. Source processors read
     * it and when they see changed value, they start a snapshot with that
     * ID. {@code Long.MIN_VALUE} means no snapshot is in progress.
     */
    private volatile long currentSnapshotId = NO_SNAPSHOT;

    /**
     * Current number of {@link StoreSnapshotTasklet}s in the job. It's
     * decremented as the tasklets complete (this is when they receive
     * DONE_ITEM and after all pending async ops completed).
     */
    private int numTasklets = Integer.MIN_VALUE;

    /**
     * Remaining number of sinks in currently produced snapshot. When it is
     * decreased to 0, the snapshot is complete.
     */
    private final AtomicInteger numRemainingTasklets = new AtomicInteger();

    // future which will be completed when the current snapshot completes
    private volatile CompletableFuture<Void> future;

    private final ProcessingGuarantee guarantee;


    SnapshotContext(ProcessingGuarantee guarantee) {
        this.guarantee = guarantee;
    }

    long currentSnapshotId() {
        return currentSnapshotId;
    }

    ProcessingGuarantee processingGuarantee() {
        return guarantee;
    }

    void initTaskletCount(int count) {
        assert this.numTasklets == Integer.MIN_VALUE : "Tasklet count already set once.";
        this.numTasklets = count;
    }

    /**
     * This method is called when the member received {@link
     * SnapshotOperation}.
     */
    synchronized CompletableFuture<Void> startNewSnapshot(long snapshotId) {
        assert snapshotId == currentSnapshotId + 1
                : "new snapshotId not incremented by 1. Previous=" + currentSnapshotId + ", new=" + snapshotId;

        if (numTasklets == 0) {
            return CompletableFuture.completedFuture(null);
        }
        boolean success = numRemainingTasklets.compareAndSet(0, numTasklets);
        assert success : "previous snapshot was not finished, numRemainingTasklets=" + numRemainingTasklets.get();
        currentSnapshotId = snapshotId;
        return (future = new CompletableFuture<>());
    }

    /**
     * Called when StoreSnapshotTasklet is done
     * @param lastSnapshotId last snapshot taken by tasklet
     */
    synchronized void taskletDone(long lastSnapshotId) {
        assert numTasklets > 0;
        assert lastSnapshotId <= currentSnapshotId;

        numTasklets--;
        if (currentSnapshotId < lastSnapshotId) {
            snapshotDoneForTasklet();
        }
    }

    /**
     * Called when current snapshot is done
     */
    void snapshotDoneForTasklet() {
        if (numRemainingTasklets.decrementAndGet() == 0) {
            future.complete(null);
            future = null;
        }
    }

}
