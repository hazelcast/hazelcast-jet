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

import com.hazelcast.jet.impl.execution.BlockingProcessorTasklet.JobFutureCompleted;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.execution.ExecutionService.IDLER;

public class BlockingSnapshotStorageImpl extends SnapshotStorageImpl {
    private CompletableFuture<Void> jobFuture;

    BlockingSnapshotStorageImpl(SerializationService serializationService, Queue<Object> snapshotQueue) {
        super(serializationService, snapshotQueue);
    }

    void initJobFuture(CompletableFuture<Void> jobFuture) {
        this.jobFuture = jobFuture;
    }

    @Override
    public boolean offer(Object key, Object value) {
        for (long idleCount = 0; ; idleCount++) {
            if (super.offer(key, value)) {
                break;
            }
            if (jobFuture.isDone()) {
                throw new JobFutureCompleted();
            }
            IDLER.idle(++idleCount);
        }
        return true;
    }
}
