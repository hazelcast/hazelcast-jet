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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.partition.IPartitionService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StoreSnapshotTasklet implements Tasklet {

    private static final String JET_SNAPSHOT_PREFIX = "__jet_snapshot.";
    private static final int MAX_PARALLEL_ASYNC_OPS = 1000;

    // These magic values are copied from com.hazelcast.spi.impl.operationservice.impl.InvokeOnPartitions
    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private final ProgressTracker progTracker = new ProgressTracker();
    private final long jobId;
    private final InboundEdgeStream inboundEdgeStream;
    private final MapEntries[] outputBuffer;

    private final IPartitionService partitionService;
    private final OperationService operationService;
    private final MapService mapService;
    private final JetService jetService;
    private final SnapshotState snapshotState;

    private long currentSnapshotId = -1;
    private long completedSnapshotId = -1;
    private final AtomicInteger ourPendingAsyncOps = new AtomicInteger();
    private boolean haveUnsentEntries;

    private final String vertexId;
    private volatile boolean inputExhausted;
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private ExecutionCallback<Object> executionCallback;

    public StoreSnapshotTasklet(SnapshotState snapshotState, long jobId, InboundEdgeStream inboundEdgeStream,
                                NodeEngine nodeEngine, String vertexId) {
        this.snapshotState = snapshotState;
        this.jobId = jobId;
        this.inboundEdgeStream = inboundEdgeStream;
        this.vertexId = vertexId;
        this.jetService = nodeEngine.getService(JetService.SERVICE_NAME);

        partitionService = nodeEngine.getPartitionService();
        operationService = nodeEngine.getOperationService();
        outputBuffer = new MapEntries[partitionService.getPartitionCount()];
        mapService = nodeEngine.getService(MapService.SERVICE_NAME);

        executionCallback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                PartitionResponse r = (PartitionResponse) response;
                for (Object result : r.getResults()) {
                    if (result instanceof Throwable) {
                        // TODO retry the operation, similarly to what putAll does?
                        firstFailure.compareAndSet(null, (Throwable) result);
                    }
                }
                jetService.getParallelAsyncOpsCounter().decrementAndGet();
                if (ourPendingAsyncOps.decrementAndGet() == 0) {
                    snapshotState.snapshotCompletedInProcessor();
                    if (inputExhausted) {
                        snapshotState.processorCompleted();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                firstFailure.compareAndSet(null, t);
            }
        };
    }

    @Nonnull
    @Override
    public ProgressState call() {
        if (firstFailure.get() != null) {
            throw new JetException("Failure during snapshot saving", firstFailure.get());
        }

        progTracker.reset();
        if (!inputExhausted) {
            progTracker.notDone();
        }

        // drain input queues
        if (!inputExhausted && (currentSnapshotId > completedSnapshotId || ourPendingAsyncOps.get() == 0)) {
            ProgressState inputQueueResult = inboundEdgeStream.drainTo(item -> {
                progTracker.madeProgress();
                if (item instanceof SnapshotStartBarrier) {
                    currentSnapshotId = ((SnapshotStartBarrier) item).snapshotId;
                } else if (item instanceof SnapshotBarrier) {
                    completedSnapshotId = ((SnapshotBarrier) item).snapshotId;
                } else {
                    haveUnsentEntries = true;
                    Entry<Data, Data> entry = (Entry<Data, Data>) item;
                    int partitionId = partitionService.getPartitionId(entry.getKey());
                    MapEntries entries = outputBuffer[partitionId];
                    if (entries == null) {
                        entries = new MapEntries();
                        outputBuffer[partitionId] = entries;
                    }
                    entries.add(entry.getKey(), entry.getValue());
                }
            });
            inputExhausted = inputQueueResult.isDone();
            if (inputExhausted && ourPendingAsyncOps.get() == 0) {
                snapshotState.processorCompleted();
            }
        }

        if (!haveUnsentEntries) {
            return progTracker.toProgressState();
        }

        // TODO check: is getMemberPartitionsMap() guaranteed to contain all partitions in the returned map?
        // TODO check: javadoc for getMemberPartitionsMap() says it might block: is this a concern? is it just upon a start-up?
        Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

        // Try to reserve room for number of async operations equal to number of members.
        // Logic is similar to AtomicInteger.updateAndGet, but it stops, when in some iteration
        // the value would exceed MAX_PARALLEL_ASYNC_OPS
        int prev;
        int next;
        do {
            prev = jetService.getParallelAsyncOpsCounter().get();
            next = prev + memberPartitionsMap.size();
            // not enough operations for us, back off
            if (next > MAX_PARALLEL_ASYNC_OPS) {
                return ProgressState.NO_PROGRESS;
            }
        } while (!jetService.getParallelAsyncOpsCounter().compareAndSet(prev, next));
        ourPendingAsyncOps.addAndGet(memberPartitionsMap.size());

        // invoke the operations
        int emptyCount = 0;
        MapOperationProvider operationProvider = mapService.getMapServiceContext().getMapOperationProvider(mapName());
        for (Entry<Address, List<Integer>> memberPartitionsEntry : memberPartitionsMap.entrySet()) {
            MapEntries[] entriesForMember = new MapEntries[memberPartitionsEntry.getValue().size()];
            int[] partitionsForMember = new int[memberPartitionsEntry.getValue().size()];
            int index = 0;
            for (Integer partition : memberPartitionsEntry.getValue()) {
                if (outputBuffer[partition] != null) {
                    entriesForMember[index] = outputBuffer[partition];
                    partitionsForMember[index] = partition;
                    index++;
                }
            }
            if (index == 0) {
                // no entries for this member, ship the operation
                emptyCount++;
                continue;
            }
            // trim arrays to real sizes
            if (index < memberPartitionsEntry.getValue().size()) {
                entriesForMember = Arrays.copyOf(entriesForMember, index);
                partitionsForMember = Arrays.copyOf(partitionsForMember, index);
            }
            // send the operation
            OperationFactory factory = operationProvider.createPutAllOperationFactory(mapName(),
                    partitionsForMember, entriesForMember);
            PartitionIteratingOperation operation =
                    new PartitionIteratingOperation(factory, memberPartitionsEntry.getValue());

            operationService
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, memberPartitionsEntry.getKey())
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .setExecutionCallback(executionCallback)
                    .invoke();

            Arrays.fill(outputBuffer, null);
        }

        // release operations for members which did not have any data
        jetService.getParallelAsyncOpsCounter().getAndAdd(-emptyCount);
        ourPendingAsyncOps.getAndAdd(-emptyCount);

        return progTracker.toProgressState();
    }

    private String mapName() {
        return JET_SNAPSHOT_PREFIX + jobId + '.' + currentSnapshotId + '.' + vertexId;
    }

    public String getVertexId() {
        return vertexId;
    }

    @Override
    public String toString() {
        return StoreSnapshotTasklet.class.getSimpleName() + ", vertex:" + vertexId;
    }
}
