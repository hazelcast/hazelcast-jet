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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.execution.BroadcastEntry;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CancelExecutionOperation;
import com.hazelcast.jet.impl.operation.CompleteExecutionOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.SnapshotRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.execution.SnapshotContext.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologicalFailure;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;
import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

/**
 * Data pertaining to single job on master member. There's one instance per job,
 * shared between multiple executions.
 */
public class MasterContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final JobRecord jobRecord;
    private final long jobId;
    private final NonCompletableFuture completionFuture = new NonCompletableFuture();
    private final CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();
    private final AtomicReference<JobStatus> jobStatus = new AtomicReference<>(NOT_STARTED);
    private final SnapshotRepository snapshotRepository;
    private volatile Set<Vertex> vertices;

    private volatile long executionId;
    private volatile long jobStartTime;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, JobRecord jobRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
        this.snapshotRepository = coordinationService.snapshotRepository();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRecord = jobRecord;
        this.jobId = jobRecord.getJobId();
    }

    public long getJobId() {
        return jobId;
    }

    public long getExecutionId() {
        return executionId;
    }

    public JobStatus jobStatus() {
        return jobStatus.get();
    }

    public JobConfig getJobConfig() {
        return jobRecord.getConfig();
    }

    public JobRecord getJobRecord() {
        return jobRecord;
    }

    public CompletableFuture<Void> completionFuture() {
        return completionFuture;
    }

    boolean cancel() {
        return cancellationFuture.cancel(true);
    }

    boolean isCancelled() {
        return cancellationFuture.isCancelled();
    }

    /**
     * Starts execution of the job if it is not already completed, cancelled or failed.
     * If the job is already cancelled, the job completion procedure is triggered.
     * If the job quorum is not satisfied, job restart is rescheduled.
     * If there was a membership change and the partition table is not completely
     * fixed yet, job restart is rescheduled.
     */
    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        if (!setJobStatusToStarting()) {
            return;
        }

        if (scheduleRestartIfQuorumAbsent() || scheduleRestartIfClusterIsNotSafe()) {
            return;
        }

        DAG dag = deserializeDAG();
        // save a copy of the vertex list, because it is going to change
        vertices = new HashSet<>();
        dag.iterator().forEachRemaining(vertices::add);
        executionId = executionIdSupplier.apply(jobId);

        // last started snapshot complete or not complete. The next started snapshot must be greater than this number
        long lastSnapshotId = NO_SNAPSHOT;
        if (isSnapshottingEnabled()) {
            Long snapshotIdToRestore = snapshotRepository.latestCompleteSnapshot(jobId);
            snapshotRepository.deleteAllSnapshotsExceptOne(jobId, snapshotIdToRestore);
            Long lastStartedSnapshot = snapshotRepository.latestStartedSnapshot(jobId);
            if (snapshotIdToRestore != null) {
                logger.info("State of " + jobIdString() + " will be restored from snapshot "
                        + snapshotIdToRestore);
                rewriteDagWithSnapshotRestore(dag, snapshotIdToRestore);
            } else {
                logger.warning("No usable snapshot for " + jobIdString() + " found.");
            }
            if (lastStartedSnapshot != null) {
                lastSnapshotId = lastStartedSnapshot;
            }
        }

        MembersView membersView = getMembersView();
        ClassLoader previousCL = swapContextClassLoader(coordinationService.getClassLoader(jobId));
        try {
            logger.info("Start executing " + jobIdString() + ", status " + jobStatus()
                    + "\n" + dag);
            logger.fine("Building execution plan for " + jobIdString());
            executionPlanMap = createExecutionPlans(nodeEngine, membersView, dag, getJobConfig(), lastSnapshotId);
        } catch (Exception e) {
            logger.severe("Exception creating execution plan for " + jobIdString(), e);
            onCompleteStepCompleted(e);
            return;
        } finally {
            Thread.currentThread().setContextClassLoader(previousCL);
        }

        logger.fine("Built execution plans for " + jobIdString());
        Set<MemberInfo> participants = executionPlanMap.keySet();
        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitExecutionOperation(jobId, executionId, membersView.getVersion(), participants, plan);
        invoke(operationCtor, this::onInitStepCompleted, null);
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId) {
        logger.info(jobIdString() + ": restoring state from snapshotId=" + snapshotId);
        for (Vertex vertex : dag) {
            // items with keys of type BroadcastKey need to be broadcast to all processors
            DistributedFunction<Entry<Object, Object>, ?> projection = (Entry<Object, Object> e) ->
                    (e.getKey() instanceof BroadcastKey) ? new BroadcastEntry<>(e) : e;
            // We add the vertex even in case when the map is empty: this ensures, that
            // Processor.finishSnapshotRestore() method is always called on all vertices in
            // a job which is restored from a snapshot.
            String mapName = snapshotDataMapName(jobId, snapshotId, vertex.getName());
            Vertex readSnapshotVertex = dag.newVertex("__read_snapshot." + vertex.getName(),
                    readMapP(mapName, truePredicate(), projection));

            readSnapshotVertex.localParallelism(vertex.getLocalParallelism());

            int destOrdinal = dag.getInboundEdges(vertex.getName()).size();
            dag.edge(new SnapshotRestoreEdge(readSnapshotVertex, vertex, destOrdinal));
        }
    }

    /**
     * Sets job status to starting.
     * Returns false if the job start process cannot proceed.
     */
    private boolean setJobStatusToStarting() {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            logger.severe("Cannot init job " + idToString(jobId) + ": it is already " + status);
            return false;
        }

        if (cancellationFuture.isCancelled()) {
            logger.fine("Skipping init job " + idToString(jobId) + ": is already cancelled.");
            onCompleteStepCompleted(new CancellationException());
            return false;
        }

        if (status == NOT_STARTED) {
            if (!jobStatus.compareAndSet(NOT_STARTED, STARTING)) {
                logger.fine("Cannot init job " + idToString(jobId) + ": someone else is just starting it");
                return false;
            }

            jobStartTime = System.currentTimeMillis();
        }

        status = jobStatus();
        if (!(status == STARTING || status == RESTARTING)) {
            logger.severe("Cannot init job " + idToString(jobId) + ": status is " + status);
            return false;
        }

        return true;
    }

    private boolean scheduleRestartIfQuorumAbsent() {
        int quorumSize = jobRecord.getQuorumSize();
        if (coordinationService.isQuorumPresent(quorumSize)) {
            return false;
        }

        logger.fine("Rescheduling restart of job " + idToString(jobId) + ": quorum size " + quorumSize + " is not met");
        scheduleRestart();
        return true;
    }

    private boolean scheduleRestartIfClusterIsNotSafe() {
        if (coordinationService.shouldStartJobs()) {
            return false;
        }

        logger.fine("Rescheduling restart of job " + idToString(jobId) + ": cluster is not safe");
        scheduleRestart();
        return true;
    }

    private void scheduleRestart() {
        jobStatus.compareAndSet(RUNNING, RESTARTING);
        coordinationService.scheduleRestart(jobId);
    }

    private MembersView getMembersView() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        return clusterService.getMembershipManager().getMembersView();
    }

    private DAG deserializeDAG() {
        ClassLoader cl = coordinationService.getClassLoader(jobId);
        return deserializeWithCustomClassLoader(nodeEngine.getSerializationService(), cl, jobRecord.getDag());
    }

    // Called as callback when all InitOperation invocations are done
    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getInitResult(responses);

        if (error == null) {
            JobStatus status = jobStatus();

            if (!(status == STARTING || status == RESTARTING)) {
                error = new IllegalStateException("Cannot execute " + jobIdString()
                        + ": status is " + status);
            }
        }

        if (error == null) {
            invokeStartExecution();
        } else {
            invokeCompleteExecution(error);
        }
    }

    /**
     * If there is no failure, then returns null. If the job is cancelled, then returns CancellationException.
     * If there is at least one non-restartable failure, such as an exception in user code, then returns that failure.
     * Otherwise, the failure is because a job participant has left the cluster.
     * In that case, TopologyChangeException is returned so that the job will be restarted.
     */
    private Throwable getInitResult(Map<MemberInfo, Object> responses) {
        if (cancellationFuture.isCancelled()) {
            logger.fine(jobIdString() + " to be cancelled after init");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Init of " + jobIdString() + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Init of " + jobIdString() + " failed with: " + failures);

        // if there is at least one non-restartable failure, such as a user code failure, then fail the job
        // otherwise, return TopologyChangedException so that the job will be restarted
        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !isTopologicalFailure(t))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    // true -> failures, false -> success responses
    private Map<Boolean, List<Entry<MemberInfo, Object>>> groupResponses(Map<MemberInfo, Object> responses) {
        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = responses
                .entrySet()
                .stream()
                .collect(partitioningBy(e -> e.getValue() instanceof Throwable));

        grouped.putIfAbsent(true, emptyList());
        grouped.putIfAbsent(false, emptyList());

        return grouped;
    }

    // If a participant leaves or the execution fails in a participant locally, executions are cancelled
    // on the remaining participants and the callback is completed after all invocations return.
    private void invokeStartExecution() {
        jobStatus.set(RUNNING);
        logger.fine("Executing " + jobIdString());

        long executionId = this.executionId;

        AtomicBoolean cancellation = new AtomicBoolean();
        ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
            }

            @Override
            public void onFailure(Throwable t) {
                if (cancellation.compareAndSet(false, true)) {
                    cancelExecute(jobId, executionId);
                }
            }
        };

        cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
            if (e instanceof CancellationException) {
                callback.onFailure(e);
            }
        }));

        Function<ExecutionPlan, Operation> operationCtor = plan -> new StartExecutionOperation(jobId, executionId);
        invoke(operationCtor, this::onExecuteStepCompleted, callback);

        if (isSnapshottingEnabled()) {
            coordinationService.scheduleSnapshot(jobId, executionId);
        }
    }

    private void cancelExecute(long jobId, long executionId) {
        nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () -> {
            Function<ExecutionPlan, Operation> operationCtor = plan -> new CancelExecutionOperation(jobId, executionId);
            invoke(operationCtor, responses -> { }, null);
        });
    }

    void beginSnapshot(long executionId) {
        if (this.executionId != executionId) {
            // current execution is completed and probably a new execution has started
            logger.warning("Not beginning snapshot since expected execution id " + idToString(this.executionId)
                    + " does not match to " + jobAndExecutionId(jobId, executionId));
            return;
        }

        List<String> vertexNames = vertices.stream().map(Vertex::getName).collect(Collectors.toList());
        long newSnapshotId = snapshotRepository.registerSnapshot(jobId, vertexNames);

        logger.info(String.format("Starting snapshot %s for %s", newSnapshotId, jobAndExecutionId(jobId, executionId)));
        Function<ExecutionPlan, Operation> factory =
                plan -> new SnapshotOperation(jobId, executionId, newSnapshotId);

        invoke(factory, responses -> onSnapshotCompleted(responses, executionId, newSnapshotId), null);
    }

    private void onSnapshotCompleted(Map<MemberInfo, Object> responses, long executionId, long snapshotId) {
        Map<Address, Throwable> errors = responses.entrySet().stream()
            .filter(e -> e.getValue() instanceof Throwable)
            .filter(e -> !(e.getValue() instanceof CancellationException) || !isTopologicalFailure(e.getValue()))
            .collect(Collectors.toMap(e -> e.getKey().getAddress(), e -> (Throwable) e.getValue()));

        boolean isSuccess = errors.isEmpty();
        if (!isSuccess) {
            logger.warning(jobAndExecutionId(jobId, executionId) + " snapshot " + snapshotId + " has failures: "
                    + errors);
        }
        coordinationService.completeSnapshot(jobId, executionId, snapshotId, isSuccess);
    }

    // Called as callback when all ExecuteOperation invocations are done
    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        invokeCompleteExecution(getExecuteResult(responses));
    }

    /**
     * If there is no failure, then returns null. If the job is cancelled,
     * then returns CancellationException.
     * If there is at least one non-restartable failure, such as an exception in
     * user code, then returns that failure.
     * Otherwise, the failure is because a job participant has left the cluster.
     * In that case, {@code TopologyChangeException} is returned so that the job will be restarted.
     */
    private Throwable getExecuteResult(Map<MemberInfo, Object> responses) {
        if (cancellationFuture.isCancelled()) {
            logger.fine(jobIdString() + " to be cancelled after execute");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Execute of " + jobIdString() + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Execute of " + jobIdString() + " has failures: " + failures);

        // If there is no user-code exception, it means at least one job participant has left the cluster.
        // In that case, all remaining participants return a CancellationException.
        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException || isTopologicalFailure(t)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private void invokeCompleteExecution(Throwable error) {
        JobStatus status = jobStatus();

        Throwable finalError;
        if (status == STARTING || status == RESTARTING || status == RUNNING) {
            logger.fine("Completing " + jobIdString());
            finalError = error;
        } else {
            if (error != null) {
                logger.severe("Cannot properly complete failed " + jobIdString()
                        + ": status is " + status, error);
            } else {
                logger.severe("Cannot properly complete " + jobIdString()
                        + ": status is " + status);
            }

            finalError = new IllegalStateException("Job coordination failed.");
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new CompleteExecutionOperation(executionId, finalError);
        invoke(operationCtor, responses -> onCompleteStepCompleted(error), null);
    }

    // Called as callback when all CompleteOperation invocations are done
    private void onCompleteStepCompleted(@Nullable Throwable failure) {
        if (assertJobNotAlreadyDone(failure)) {
            return;
        }

        completeVertices(failure);

        long completionTime = System.currentTimeMillis();
        if (failure instanceof TopologyChangedException && jobRecord.getConfig().isAutoRestartOnMemberFailureEnabled()) {
            scheduleRestart();
            return;
        }

        long elapsed = completionTime - jobStartTime;
        if (isSuccess(failure)) {
            logger.info("Execution of " + jobIdString() + " completed in " + elapsed + " ms");
        } else {
            logger.warning("Execution of " + jobIdString()
                    + " failed in " + elapsed + " ms", failure);
        }

        try {
            coordinationService.completeJob(this, executionId, completionTime, failure);
        } catch (RuntimeException e) {
            logger.warning("Completion of " + jobIdString()
                    + " failed in " + elapsed + " ms", failure);
        } finally {
            setFinalResult(failure);
        }
    }

    private boolean assertJobNotAlreadyDone(@Nullable Throwable failure) {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            if (failure != null) {
                logger.severe("Ignoring failure completion of " + idToString(jobId) + " because status is "
                        + status, failure);
            } else {
                logger.severe("Ignoring completion of " + idToString(jobId) + " because status is " + status);
            }
            return true;
        }
        return false;
    }

    private void completeVertices(@Nullable Throwable failure) {
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                try {
                    vertex.getMetaSupplier().complete(failure);
                } catch (Exception e) {
                    logger.severe(jobIdString()
                            + " encountered an exception in ProcessorMetaSupplier.complete(), ignoring it", e);
                }
            }
        }
    }

    void setFinalResult(Throwable failure) {
        JobStatus status = isSuccess(failure) ? COMPLETED : FAILED;
        jobStatus.set(status);
        if (failure == null) {
            completionFuture.internalComplete();
        } else {
            completionFuture.internalCompleteExceptionally(failure);
        }
    }

    private boolean isSuccess(Throwable failure) {
        return (failure == null || failure instanceof CancellationException);
    }

    private void invoke(Function<ExecutionPlan, Operation> operationCtor,
                        Consumer<Map<MemberInfo, Object>> completionCallback,
                        ExecutionCallback<Object> callback) {
        CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        Map<MemberInfo, InternalCompletableFuture<Object>> futures = new ConcurrentHashMap<>();
        invokeOnParticipants(futures, doneFuture, operationCtor);

        // once all invocations return, notify the completion callback
        doneFuture.whenComplete(withTryCatch(logger, (aVoid, throwable) -> {
            Map<MemberInfo, Object> responses = new HashMap<>();
            for (Entry<MemberInfo, InternalCompletableFuture<Object>> entry : futures.entrySet()) {
                Object val;
                try {
                    val = entry.getValue().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    val = e;
                } catch (Exception e) {
                    val = peel(e);
                }
                responses.put(entry.getKey(), val);
            }
            completionCallback.accept(responses);
        }));

        if (callback != null) {
            futures.values().forEach(f -> f.andThen(callback));
        }
    }

    private void invokeOnParticipants(Map<MemberInfo, InternalCompletableFuture<Object>> futures,
                                      CompletableFuture<Void> doneFuture,
                                      Function<ExecutionPlan, Operation> opCtor) {
        AtomicInteger remainingCount = new AtomicInteger(executionPlanMap.size());
        for (Entry<MemberInfo, ExecutionPlan> e : executionPlanMap.entrySet()) {
            MemberInfo member = e.getKey();
            Operation op = opCtor.apply(e.getValue());
            InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                 .createInvocationBuilder(JetService.SERVICE_NAME, op, member.getAddress())
                 .setDoneCallback(() -> {
                     if (remainingCount.decrementAndGet() == 0) {
                         doneFuture.complete(null);
                     }
                 })
                 .invoke();
            futures.put(member, future);
        }
    }

    private boolean isSnapshottingEnabled() {
        return getJobConfig().getProcessingGuarantee() != ProcessingGuarantee.NONE;
    }

    private String jobIdString() {
        return jobAndExecutionId(jobId, executionId);
    }

    private static ClassLoader swapContextClassLoader(ClassLoader jobClassLoader) {
        Thread currentThread = Thread.currentThread();
        ClassLoader contextClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(jobClassLoader);
        return contextClassLoader;
    }

    /**
     * Specific type of edge to be used when restoring snapshots
     */
    private static class SnapshotRestoreEdge extends Edge {

        SnapshotRestoreEdge(Vertex source, Vertex destination, int destOrdinal) {
            super(source, 0, destination, destOrdinal);
            distributed();
            partitioned(entryKey());
        }

        @Override
        public int getPriority() {
            return SNAPSHOT_RESTORE_EDGE_PRIORITY;
        }
    }

}
