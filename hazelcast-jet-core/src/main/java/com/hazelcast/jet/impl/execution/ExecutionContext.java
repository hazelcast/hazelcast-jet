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

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class ExecutionContext {

    private final long jobId;
    private final long executionId;
    private final Address coordinator;
    private final Set<Address> participants;
    private final Object executionLock = new Object();
    private final ILogger logger;

    // dest vertex id --> dest ordinal --> sender addr --> receiver tasklet
    private Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap = emptyMap();

    // dest vertex id --> dest ordinal --> dest addr --> sender tasklet
    private Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = emptyMap();

    private List<ProcessorSupplier> procSuppliers = emptyList();
    private List<Processor> processors = emptyList();

    private List<Tasklet> tasklets;
    private final CompletableFuture<Void> jobFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> doneFuture = new CompletableFuture<>();

    private final NodeEngine nodeEngine;
    private final TaskletExecutionService execService;
    private SnapshotContext snapshotContext;

    public ExecutionContext(NodeEngine nodeEngine, TaskletExecutionService execService,
                            long jobId, long executionId, Address coordinator, Set<Address> participants) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.coordinator = coordinator;
        this.participants = new HashSet<>(participants);
        this.execService = execService;
        this.nodeEngine = nodeEngine;

        logger = nodeEngine.getLogger(getClass());
    }

    public ExecutionContext initialize(ExecutionPlan plan) {
        // Must be populated early, so all processor suppliers are
        // available to be completed in the case of init failure
        procSuppliers = unmodifiableList(plan.getProcessorSuppliers());
        processors = plan.getProcessors();
        snapshotContext = new SnapshotContext(nodeEngine.getLogger(SnapshotContext.class), jobId, executionId,
                plan.lastSnapshotId(), plan.getJobConfig().getProcessingGuarantee());
        plan.initialize(nodeEngine, jobId, executionId, snapshotContext);
        snapshotContext.initTaskletCount(plan.getStoreSnapshotTaskletCount(), plan.getHigherPriorityVertexCount());
        receiverMap = unmodifiableMap(plan.getReceiverMap());
        senderMap = unmodifiableMap(plan.getSenderMap());
        tasklets = plan.getTasklets();
        return this;
    }

    public void beginExecution() {
        synchronized (executionLock) {
            // check for job completion in case the job was cancelled before execute() was called.
            if (!jobFuture.isDone()) {
                JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
                ClassLoader cl = service.getClassLoader(jobId);
                doneFuture.whenComplete(withTryCatch(logger, (r, e) -> tasklets.clear()));
                execService.beginExecute(tasklets, jobFuture, doneFuture, cl);
            }
        }
    }

    public boolean cancelExecution() {
        return jobFuture.cancel(true);
    }

    public CompletableFuture<Void> doneFuture() {
        return doneFuture;
    }

    public CompletableFuture<Void> jobFuture() {
        return jobFuture;
    }

    public long jobId() {
        return jobId;
    }

    public long executionId() {
        return executionId;
    }

    public Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap() {
        return senderMap;
    }

    public Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap() {
        return receiverMap;
    }

    public boolean verify(Address coordinator, long jobId) {
        return this.coordinator.equals(coordinator) && this.jobId == jobId;
    }

    // should not leak exceptions thrown by processor suppliers
    public void complete(Throwable error) {
        ILogger logger = nodeEngine.getLogger(getClass());
        procSuppliers.forEach(s -> {
            try {
                s.complete(error);
            } catch (Throwable e) {
                logger.severe(jobAndExecutionId(jobId, executionId)
                        + " encountered an exception in ProcessorSupplier.complete(), ignoring it", e);
            }
        });
        MetricsRegistry metricsRegistry = ((NodeEngineImpl) nodeEngine).getMetricsRegistry();
        processors.forEach(metricsRegistry::deregister);
    }

    public void handlePacket(int vertexId, int ordinal, Address sender, BufferObjectDataInput in) {
        receiverMap.get(vertexId)
                   .get(ordinal)
                   .get(sender)
                   .receiveStreamPacket(in);
    }

    public CompletionStage<Void> beginSnapshot(long snapshotId) {
        synchronized (executionLock) {
            if (jobFuture.isDone()) {
                throw new CancellationException();
            }
            return snapshotContext.startNewSnapshot(snapshotId);
        }
    }

    public boolean isParticipating(Address member) {
        return participants.contains(member);
    }

    public Address getCoordinator() {
        return coordinator;
    }

    public boolean isCoordinator(Address member) {
        return coordinator.equals(member);
    }

    public SnapshotContext getSnapshotContext() {
        return snapshotContext;
    }
}
