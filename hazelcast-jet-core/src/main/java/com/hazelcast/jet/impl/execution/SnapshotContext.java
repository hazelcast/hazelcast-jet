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

public class SnapshotContext {

    private static final int NO_SNAPSHOT = -1;

    /**
     * SnapshotId of snapshot currently being created. Source processors read
     * it and when they see changed value, they start a snapshot with that
     * ID. {@code Long.MIN_VALUE} means no snapshot is in progress.
     */
    // is volatile because it will be read without synchronizing
    private volatile long currentSnapshotId = NO_SNAPSHOT;

    /**
     * Current number of {@link StoreSnapshotTasklet}s in the job. It's
     * decremented as the tasklets complete (this is when they receive
     * DONE_ITEM and after all pending async ops completed).
     */
    private int taskletCount = Integer.MIN_VALUE;

    /**
     * Remaining number of sinks in currently produced snapshot. When it is
     * decreased to 0, the snapshot is complete.
     */
    // this is an AtomicInteger because it will be decremented without synchronizing
    private final AtomicInteger remainingProcessors = new AtomicInteger();

    private final ProcessingGuarantee guarantee;

    private CompletableFuture<Void> future;

    SnapshotContext(ProcessingGuarantee guarantee) {
        this.guarantee = guarantee;
    }

    void initTaskletCount(int count) {
        assert this.taskletCount == Integer.MIN_VALUE;
        this.taskletCount = count;
    }

    long getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    /**
     * This method is called when the member received {@link
     * SnapshotOperation}.
     */
    synchronized CompletableFuture<Void> startNewSnapshot(long snapshotId) {
        assert remainingProcessors.get() == 0
                : "previous snapshot not finished, remainingProcessors=" + remainingProcessors.get();
        assert snapshotId == currentSnapshotId + 1
                : "new snapshotId not incremented by 1. Previous=" + currentSnapshotId + ", new=" + snapshotId;
        assert future == null;

        remainingProcessors.set(taskletCount);
        currentSnapshotId = snapshotId;

        CompletableFuture<Void> localFuture = new CompletableFuture<>();
        if (taskletCount == 0) {
            localFuture.complete(null);
        } else {
            future = localFuture;
        }
        return localFuture;
    }

    synchronized void processorCompleted() {
        assert taskletCount > 0;
        taskletCount--;
    }

    void snapshotCompletedInProcessor() {
        int res = remainingProcessors.decrementAndGet();
        assert res >= 0;
        if (res == 0) {
            future.complete(null);
            future = null;
        }
    }

    public ProcessingGuarantee getGuarantee() {
        return guarantee;
    }
}
