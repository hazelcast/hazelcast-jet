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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.util.CollectionUtil.toIntegerList;

/**
 * Utility for cooperative writes to a Map
 */
public class AsyncMapWriter {

    public static final int MAX_PARALLEL_ASYNC_OPS = 1000;

    // These magic values are copied from com.hazelcast.spi.impl.operationservice.impl.InvokeOnPartitions
    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private final IPartitionService partitionService;
    private final OperationService operationService;
    private final MapService mapService;
    private final SerializationService serializationService;

    private final MapEntries[] outputBuffers; // one buffer per partition
    private final AtomicInteger numConcurrentOps; // num concurrent ops across whole instance

    private String mapName;
    private MapOperationProvider opProvider;

    public AsyncMapWriter(NodeEngine nodeEngine) {
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.outputBuffers = new MapEntries[partitionService.getPartitionCount()];
        this.serializationService = nodeEngine.getSerializationService();

        JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.numConcurrentOps = jetService.numConcurrentPutAllOps();
    }

    public void put(Map.Entry<Data, Data> entry) {
        int partitionId = partitionService.getPartitionId(entry.getKey());
        MapEntries entries = outputBuffers[partitionId];
        if (entries == null) {
            entries = outputBuffers[partitionId] = new MapEntries();
        }
        entries.add(entry.getKey(), entry.getValue());
    }

    public void put(Object key, Object value) {
        // TODO use Map's partitioning strategy, as in MapProxySupport.toDataWithStrategy
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);
        put(entry(keyData, valueData));
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
        this.opProvider = mapService.getMapServiceContext().getMapOperationProvider(mapName);
    }

    public boolean tryFlushAsync(CompletableFuture<Void> completionFuture) {
        Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();
        // Try to reserve room for number of async operations equal to number of members.
        // Logic is similar to AtomicInteger.updateAndGet, but it stops, when in some iteration
        // the value would exceed MAX_PARALLEL_ASYNC_OPS
        if (!reserveOps(memberPartitionsMap.size())) {
            return false;
        }

        List<PartitionOpHolder> ops = memberPartitionsMap.entrySet()
                           .stream()
                           .map(e -> getPartitionOp(e.getKey(), e.getValue()))
                           .filter(Objects::nonNull)
                           .collect(Collectors.toList());

        for (Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
            PartitionOpHolder holder = getPartitionOp(entry.getKey(), entry.getValue());
            if (holder == null) {
                continue;
            }
            ops.add(holder);
        }

        // release operations for members which did not have any data
        releaseOps(memberPartitionsMap.size() - ops.size());
        invokeOnCluster(ops, completionFuture);
        resetBuffers();
        return true;
    }

    private PartitionOpHolder getPartitionOp(Address member, List<Integer> partitions) {
        PartitionOpHolder holder = new PartitionOpHolder();
        holder.entries = new MapEntries[partitions.size()];
        holder.partitions = new int[partitions.size()];
        int index = 0;
        for (Integer partition : partitions) {
            if (outputBuffers[partition] != null) {
                holder.entries[index] = outputBuffers[partition];
                holder.partitions[index] = partition;
                index++;
            }
        }
        if (index == 0) {
            // no entries for this member, skip the member
            return null;
        }

        // trim arrays to real sizes
        if (index < partitions.size()) {
            holder.entries = Arrays.copyOf(holder.entries, index);
            holder.partitions = Arrays.copyOf(holder.partitions, index);
        }

        OperationFactory factory = opProvider.createPutAllOperationFactory(mapName,
                holder.partitions, holder.entries);
        holder.op = new PartitionIteratingOperation(factory, toIntegerList(holder.partitions));
        holder.address = member;
        return holder;
    }

    private void resetBuffers() {
        Arrays.fill(outputBuffers, null);
    }

    private void releaseOps(int count) {
        if (count > 0) {
            numConcurrentOps.getAndAdd(-count);
        }
    }

    private boolean reserveOps(int count) {
        int prev;
        int next;
        do {
            prev = numConcurrentOps.get();
            next = prev + count;
            if (next > MAX_PARALLEL_ASYNC_OPS) {
                return false;
            }
        } while (!numConcurrentOps.compareAndSet(prev, next));
        return true;
    }

    private void invokeOnCluster(List<PartitionOpHolder> opHolders, CompletableFuture<Void> completionFuture) {
        AtomicInteger doneLatch = new AtomicInteger(opHolders.size());
        for (PartitionOpHolder holder : opHolders) {
            ExecutionCallback<PartitionResponse> callback = callbackOf(r -> {
                numConcurrentOps.decrementAndGet();
                System.out.println("Callback " + r);
                for (Object o : r.getResults()) {
                    if (o instanceof Throwable) {
                        //TODO: retry failed partitions
                        completionFuture.completeExceptionally((Throwable) o);
                        return;
                    }
                }
                if (doneLatch.decrementAndGet() == 0) {
                    completionFuture.complete(null);
                }
            }, throwable -> {
                numConcurrentOps.decrementAndGet();
                if (throwable instanceof RetryableException) {
                    //TODO: the whole operation on the member failed
                }
                completionFuture.completeExceptionally(throwable);
            });
            operationService
                    .createInvocationBuilder(MapService.SERVICE_NAME, holder.op, holder.address)
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .setExecutionCallback((ExecutionCallback) callback)
                    .invoke();
        }
    }

    private <T> ExecutionCallback<T> callbackOf(Consumer<T> onResponse, Consumer<Throwable> onError) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                onResponse.accept(o);
            }

            @Override
            public void onFailure(Throwable throwable) {
                onError.accept(throwable);
            }
        };
    }

    private class PartitionOpHolder {
        private Address address;
        private PartitionIteratingOperation op;

        // PartitionIteratingOp doesn't expose these, so we have to track them separately
        private MapEntries[] entries; //entries in the operation
        private int[] partitions; // partitions in the operation

    }
}

