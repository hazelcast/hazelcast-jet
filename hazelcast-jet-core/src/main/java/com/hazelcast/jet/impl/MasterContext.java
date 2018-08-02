/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate;
import com.hazelcast.jet.impl.exception.JobRestartRequestedException;
import com.hazelcast.jet.impl.exception.JobSuspendRequestedException;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.exception.ShutdownInProgressException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CompleteExecutionOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
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

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.SnapshotRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.RESTART;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_GRACEFUL;
import static com.hazelcast.jet.impl.execution.SnapshotContext.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.jobNameAndExecutionId;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

/**
 * Data pertaining to single job on master member. There's one instance per job,
 * shared between multiple executions.
 */
public class MasterContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;
    public static final String SNAPSHOT_VERTEX_PREFIX = "__snapshot_";

    private final Object lock = new Object();

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final JobRecord jobRecord;
    private final long jobId;
    private final String jobName;
    private final AtomicReference<JobStatus> jobStatus = new AtomicReference<>(NOT_STARTED);
    private final SnapshotRepository snapshotRepository;
    private volatile Set<Vertex> vertices;

    private volatile long executionId;
    private volatile long executionStartTime;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;
    private volatile ExecutionInvocationCallback executionInvocationCallback;

    /**
     * A future completed when the job fully completes. It's NOT completed when
     * the job is suspended or when it is going to be restarted. It's used for
     * {@link Job#join()}.
     */
    private final NonCompletableFuture completionFuture = new NonCompletableFuture();

    /**
     * Null initially. When a job termination is requested, it is assigned a
     * termination mode. It's reset back to null when execute operations
     * complete.
     */
    private final AtomicReference<TerminationMode> requestedTerminationMode = new AtomicReference<>();

    /**
     * It's true while a snapshot is in progress. It's used to prevent
     * concurrent snapshots.
     */
    private final AtomicBoolean snapshotInProgress = new AtomicBoolean();

    /**
     * If {@code true}, the snapshot that will be executed next will be
     * terminal (a graceful shutdown). It is set to true when a graceful
     * shutdown or restart is requested and reset back to false when such a
     * snapshot is initiated.
     *
     * <p>If it's true at snapshot completion time, the next snapshot is not
     * scheduled after a delay but run immediately.
     */
    private volatile boolean nextSnapshotIsTerminal;

    /**
     * A future (re)created when the job is started and completed when terminal
     * snapshot is completed (successfully or not).
     */
    private CompletableFuture<Void> terminalSnapshotFuture;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, JobRecord jobRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
        this.snapshotRepository = coordinationService.snapshotRepository();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRecord = jobRecord;
        this.jobId = jobRecord.getJobId();
        this.jobName = jobRecord.getJobNameOrId();
        if (jobRecord.isSuspended()) {
            jobStatus.set(SUSPENDED);
        }
    }

    public long jobId() {
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

    JobRecord getJobRecord() {
        return jobRecord;
    }

    public CompletableFuture<Void> completionFuture() {
        return completionFuture;
    }

    /**
     * @return false, if termination was already requested
     */
    boolean requestTermination(TerminationMode mode) {
        synchronized (lock) {
            if (!isSnapshottingEnabled()) {
                // switch graceful method to forceful if we don't do snapshots
                mode = mode.withoutTerminalSnapshot();
            }

            JobStatus jobStatus = jobStatus();
            boolean success = requestedTerminationMode.compareAndSet(null, mode);
            if (success) {
                handleTermination(mode);
                // handle cancellation of a suspended job
                if (jobStatus == SUSPENDED) {
                    assert mode == CANCEL : "mode is not CANCEL, but " + mode;
                    coordinationService.completeJob(this, System.currentTimeMillis(), new CancellationException());
                    this.jobStatus.set(COMPLETED);
                }
            }
            return success;
        }
    }

    boolean isCancelled() {
        return requestedTerminationMode.get() == CANCEL;
    }

    TerminationMode requestedTerminationMode() {
        return requestedTerminationMode.get();
    }

    /**
     * Starts execution of the job if it is not already completed, cancelled or failed.
     * If the job is already cancelled, the job completion procedure is triggered.
     * If the job quorum is not satisfied, job restart is rescheduled.
     * If there was a membership change and the partition table is not completely
     * fixed yet, job restart is rescheduled.
     */
    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        synchronized (lock) {
            if (!setJobStatusToStarting()
                    || scheduleRestartIfQuorumAbsent()
                    || scheduleRestartIfClusterIsNotSafe()) {
                return;
            }

            TerminationMode terminationMode = requestedTerminationMode.get();
            if (terminationMode != null) {
                if (terminationMode.actionAfterTerminate() == RESTART) {
                    // ignore restart, we are just starting
                    requestedTerminationMode.set(null);
                } else {
                    finalizeJob(terminationMode.createException());
                    return;
                }
            }

            SerializationService serializationService = nodeEngine.getSerializationService();
            ClassLoader classLoader = coordinationService.getJetService().getClassLoader(jobId);
            DAG dag;
            try {
                dag = deserializeWithCustomClassLoader(serializationService, classLoader, jobRecord.getDag());
            } catch (Exception e) {
                logger.warning("DAG deserialization failed", e);
                finalizeJob(e);
                return;
            }
            // save a copy of the vertex list because it is going to change
            vertices = new HashSet<>();
            dag.iterator().forEachRemaining(vertices::add);
            executionId = executionIdSupplier.apply(jobId);

            snapshotInProgress.set(false);
            nextSnapshotIsTerminal = false;
            terminalSnapshotFuture = new CompletableFuture<>();

            // last started snapshot, completed or not. The next started snapshot must be greater than this number
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
                    logger.info("No previous snapshot for " + jobIdString() + " found.");
                }
                if (lastStartedSnapshot != null) {
                    lastSnapshotId = lastStartedSnapshot;
                }
            }

            MembersView membersView = getMembersView();
            ClassLoader previousCL = swapContextClassLoader(classLoader);
            try {
                logger.info("Start executing " + jobIdString() + ", status " + jobStatus()
                        + ", execution graph in DOT format:\n" + dag.toDotString()
                        + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.");
                logger.fine("Building execution plan for " + jobIdString());
                executionPlanMap = createExecutionPlans(nodeEngine, membersView,
                        dag, jobId, executionId, getJobConfig(), lastSnapshotId);
            } catch (Exception e) {
                logger.severe("Exception creating execution plan for " + jobIdString(), e);
                finalizeJob(e);
                return;
            } finally {
                Thread.currentThread().setContextClassLoader(previousCL);
            }

            logger.fine("Built execution plans for " + jobIdString());
            Set<MemberInfo> participants = executionPlanMap.keySet();
            Function<ExecutionPlan, Operation> operationCtor = plan ->
                    new InitExecutionOperation(jobId, executionId, membersView.getVersion(), participants,
                            serializationService.toData(plan));
            invoke(operationCtor, this::onInitStepCompleted, null);
        }
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId) {
        logger.info(jobIdString() + ": restoring state from snapshotId=" + snapshotId);
        for (Vertex vertex : dag) {
            // We add the vertex even in case when the map is empty: this ensures, that
            // Processor.finishSnapshotRestore() method is always called on all vertices in
            // a job which is restored from a snapshot.
            String mapName = snapshotDataMapName(jobId, snapshotId, vertex.getName());
            Vertex readSnapshotVertex = dag.newVertex(
                    SNAPSHOT_VERTEX_PREFIX + "read." + vertex.getName(), readMapP(mapName)
            );
            Vertex explodeVertex = dag.newVertex(
                    SNAPSHOT_VERTEX_PREFIX + "explode." + vertex.getName(), ExplodeSnapshotP::new
            );

            readSnapshotVertex.localParallelism(vertex.getLocalParallelism());
            explodeVertex.localParallelism(vertex.getLocalParallelism());

            int destOrdinal = dag.getInboundEdges(vertex.getName()).size();
            dag.edge(between(readSnapshotVertex, explodeVertex).isolated())
               .edge(new SnapshotRestoreEdge(explodeVertex, vertex, destOrdinal));
        }
    }

    /**
     * Sets job status to starting.
     * Returns false if the job start process cannot proceed.
     */
    private boolean setJobStatusToStarting() {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED || status == SUSPENDED) {
            logger.severe("Cannot init job '" + jobName + "': it is " + status);
            return false;
        }

        if (isCancelled()) {
            logger.fine("Skipping init job '" + jobName + "': is already cancelled.");
            finalizeJob(new CancellationException());
            return false;
        }

        if (status == NOT_STARTED) {
            if (!jobStatus.compareAndSet(NOT_STARTED, STARTING)) {
                logger.fine("Cannot init job '" + jobName + "': someone else is just starting it");
                return false;
            }
        }

        executionStartTime = System.nanoTime();
        status = jobStatus();
        if (status != STARTING && status != RESTARTING) {
            logger.severe("Cannot init job '" + jobName + "': status is " + status);
            return false;
        }

        return true;
    }

    private boolean scheduleRestartIfQuorumAbsent() {
        int quorumSize = jobRecord.getQuorumSize();
        if (coordinationService.isQuorumPresent(quorumSize)) {
            return false;
        }

        logger.fine("Rescheduling restart of job '" + jobName + "': quorum size " + quorumSize + " is not met");
        scheduleRestart();
        return true;
    }

    private boolean scheduleRestartIfClusterIsNotSafe() {
        if (coordinationService.shouldStartJobs()) {
            return false;
        }

        logger.fine("Rescheduling restart of job '" + jobName + "': cluster is not safe");
        scheduleRestart();
        return true;
    }

    private void scheduleRestart() {
        // if status is RUNNING, set it to RESTARTING. If it's STARTING, leave it
        jobStatus.compareAndSet(RUNNING, RESTARTING);
        coordinationService.scheduleRestart(jobId);
    }

    private MembersView getMembersView() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        return clusterService.getMembershipManager().getMembersView();
    }

    // Called as callback when all InitOperation invocations are done
    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getResult("Init", responses);

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
        logger.fine("Executing " + jobIdString());

        long executionId = this.executionId;

        executionInvocationCallback = new ExecutionInvocationCallback(executionId);
        if (requestedTerminationMode.get() != null) {
            handleTermination(requestedTerminationMode.get());
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new StartExecutionOperation(jobId, executionId);
        Consumer<Map<MemberInfo, Object>> completionCallback = this::onExecuteStepCompleted;

        jobStatus.set(RUNNING);

        invoke(operationCtor, completionCallback, executionInvocationCallback);

        if (isSnapshottingEnabled()) {
            coordinationService.scheduleSnapshot(jobId, executionId);
        }
    }

    private void handleTermination(@Nonnull TerminationMode mode) {
        // this method can be called multiple times to handle the termination, it must
        // be safe against it (idempotent).
        if (mode.isWithTerminalSnapshot()) {
            nextSnapshotIsTerminal = true;
            beginSnapshot(executionId);
        } else {
            if (executionInvocationCallback != null) {
                executionInvocationCallback.cancelInvocations(mode);
            }
        }
    }

    private void cancelExecutionInvocations(long jobId, long executionId, TerminationMode mode) {
        nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () ->
                invoke(plan -> new TerminateExecutionOperation(jobId, executionId, mode), responses -> { }, null));
    }

    void beginSnapshot(long executionId) {
        synchronized (lock) {
            if (this.executionId != executionId) {
                // current execution is completed and probably a new execution has started
                logger.warning("Not beginning snapshot since unexpected execution ID received for " + jobIdString()
                        + ". Received execution ID: " + idToString(executionId));
                return;
            }

            if (!snapshotInProgress.compareAndSet(false, true)) {
                logger.fine("Not beginning snapshot since one is already in progress " + jobIdString());
                return;
            }

            boolean isTerminal = nextSnapshotIsTerminal;
            nextSnapshotIsTerminal = false;

            List<String> vertexNames = vertices.stream().map(Vertex::getName).collect(Collectors.toList());
            long newSnapshotId = snapshotRepository.registerSnapshot(jobId, vertexNames);

            logger.info(String.format("Starting%s snapshot %s for %s",
                    isTerminal ? " terminal" : "", newSnapshotId, jobIdString()));
            Function<ExecutionPlan, Operation> factory =
                    plan -> new SnapshotOperation(jobId, executionId, newSnapshotId, isTerminal);

            invoke(factory, responses -> onSnapshotCompleted(responses, executionId, newSnapshotId, isTerminal), null);
        }
    }

    private void onSnapshotCompleted(Map<MemberInfo, Object> responses, long executionId, long snapshotId,
                                                  boolean wasTerminal) {
        synchronized (lock) {
            // Note: this method can be called after finalizeJob() is called
            SnapshotOperationResult mergedResult = new SnapshotOperationResult();
            for (Object response : responses.values()) {
                // the response either SnapshotOperationResult or an exception, see #invoke() method
                if (response instanceof Throwable) {
                    response = new SnapshotOperationResult(0, 0, 0, (Throwable) response);
                }
                mergedResult.merge((SnapshotOperationResult) response);
            }

            boolean isSuccess = mergedResult.getError() == null;
            if (!isSuccess) {
                logger.warning(jobIdString() + " snapshot " + snapshotId + " failed on some member(s), " +
                        "one of the failures: " + mergedResult.getError());
            }
            coordinationService.completeSnapshot(jobId, snapshotId, isSuccess,
                    mergedResult.getNumBytes(), mergedResult.getNumKeys(), mergedResult.getNumChunks());
            snapshotInProgress.compareAndSet(true, false);
            if (wasTerminal) {
                boolean completedNow = terminalSnapshotFuture.complete(null);
                assert completedNow : "terminalSnapshotFuture was already completed";
            }

            if (nextSnapshotIsTerminal) {
                // If the next snapshot is terminal, start it right away.
                coordinationService.beginSnapshot(jobId, executionId);
            } else if (!wasTerminal) {
                // If the snapshot wasn't terminal, schedule next snapshot after a delay.
                // If it was terminal, no need to schedule next snapshot even if it wasn't successful,
                // because the execution already terminated.
                coordinationService.scheduleSnapshot(jobId, executionId);
            }
        }
    }

    // Called as callback when all ExecuteOperation invocations are done
    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        invokeCompleteExecution(getResult("Execution", responses));
    }

    /**
     * <ul>
     * <li>Returns null if there is no failure.
     * <li>Returns a CancellationException if the job is cancelled.
     * <li>Returns a JobRestartRequestedException if the current execution is cancelled
     * <li>Returns a JobSuspendRequestedException if the current execution is stopped
     * <li>If there is at least one non-restartable failure, such as an exception in user code, then returns that failure.
     * <li>Otherwise, the failure is because a job participant has left the cluster.
     *   In that case, {@code TopologyChangeException} is returned so that the job will be restarted.
     * </ul>
     */
    private Throwable getResult(String opName, Map<MemberInfo, Object> responses) {
        if (isCancelled()) {
            logger.fine(jobIdString() + " to be cancelled after " + opName);
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        // there was no user exception. If the termination was requested, return appropriate exception
        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine(opName + " of " + jobIdString() + " was successful");

            // TODO This is a race:
            // If the job completed normally and a termination was requested, we assume that it
            // completed normally due to the termination. We should make the members return
            // the exception in case a termination was requested and not complete normally.
            TerminationMode mode = requestedTerminationMode.get();
            // mode is null if the job completed or failed without a job-control action
            if (mode != null) {
                if (mode == CANCEL) {
                    logger.fine(jobIdString() + " to be cancelled after execute");
                    return new CancellationException();
                } else if (mode.actionAfterTerminate() == RESTART) {
                    return new JobRestartRequestedException();
                } else if (mode.actionAfterTerminate() == ActionAfterTerminate.SUSPEND) {
                    return new JobSuspendRequestedException();
                } else {
                    assert mode.actionAfterTerminate() == ActionAfterTerminate.TERMINATE :
                            "actionAfterTerminate=" + mode.actionAfterTerminate();
                    return new JobTerminateRequestedException();
                }
            }

            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine(opName + " of " + jobIdString() + " has failures: " + failures);

        // If there is no user-code exception, it means at least one job participant has left the cluster.
        // In that case, all remaining participants return a CancellationException.
        return failures
                .stream()
                .peek(e -> {
                    if (e.getValue() instanceof ShutdownInProgressException) {
                        coordinationService.addShuttingDownMember(e.getKey().getUuid());
                    }
                })
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException || isRestartableException(t)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElseGet(TopologyChangedException::new);
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
        invoke(operationCtor, responses -> finalizeJob(error), null);
    }

    // Called as callback when all CompleteOperation invocations are done
    void finalizeJob(@Nullable Throwable failure) {
        synchronized (lock) {
            if (assertJobNotAlreadyDone(failure)) {
                return;
            }
            completeVertices(failure);

            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - executionStartTime);
            boolean isSuccess = failure == null
                    || failure instanceof CancellationException
                    || failure instanceof JobRestartRequestedException
                    || failure instanceof JobSuspendRequestedException
                    || failure instanceof JobTerminateRequestedException;
            if (isSuccess) {
                logger.info(String.format("Execution of %s completed in %,d ms", jobIdString(), elapsed));
            } else {
                logger.warning(String.format("Execution of %s failed after %,d ms", jobIdString(), elapsed), failure);
            }

            // reset state for the next execution
            requestedTerminationMode.set(null);
            executionInvocationCallback = null;

            // if restart was requested, restart immediately
            if (failure instanceof JobRestartRequestedException) {
                // if status is RUNNING, set it to RESTARTING. If it's STARTING, let it be.
                jobStatus.compareAndSet(RUNNING, RESTARTING);
                coordinationService.restartJob(jobId);
            } else if ((failure instanceof RestartableException || failure instanceof TopologyChangedException)
                    && jobRecord.getConfig().isAutoScaling()) {
                // if restart is due to a failure, restart with a delay
                scheduleRestart();
            } else if (failure instanceof JobSuspendRequestedException
                    || (failure instanceof RestartableException || failure instanceof TopologyChangedException)
                    && !jobRecord.getConfig().isAutoScaling()) {
                jobStatus.set(SUSPENDED);
                coordinationService.suspendJob(this);
            } else if (failure instanceof JobTerminateRequestedException) {
                // leave the job not-suspended, not-restarted. New master will pick it up.
                jobStatus.set(NOT_STARTED);
            } else {
                jobStatus.set(isSuccess ? COMPLETED : FAILED);

                if (failure instanceof LocalMemberResetException) {
                    setFinalResult(new CancellationException());
                    logger.severe("local member reset");
                    return;
                }

                try {
                    coordinationService.completeJob(this, System.currentTimeMillis(), failure);
                } catch (RuntimeException e) {
                    logger.warning("Completion of " + jobIdString() + " failed", e);
                } finally {
                    setFinalResult(failure);
                }
            }
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
                    vertex.getMetaSupplier().close(failure);
                } catch (Exception e) {
                    logger.severe(jobIdString()
                            + " encountered an exception in ProcessorMetaSupplier.complete(), ignoring it", e);
                }
            }
        }
    }

    void setFinalResult(Throwable failure) {
        if (failure == null) {
            completionFuture.internalComplete();
        } else {
            completionFuture.internalCompleteExceptionally(failure);
        }
    }

    /**
     * @param completionCallback a consumer that will receive a map of
     *                           responses, one for each member. The value will
     *                           be either the response or an exception thrown
     *                           from the operation.
     */
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

    String jobIdString() {
        return jobNameAndExecutionId(jobName, executionId);
    }

    private static ClassLoader swapContextClassLoader(ClassLoader jobClassLoader) {
        Thread currentThread = Thread.currentThread();
        ClassLoader previous = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(jobClassLoader);
        return previous;
    }

    void resumeJob(Function<Long, Long> executionIdSupplier) {
        synchronized (lock) {
            if (jobStatus.compareAndSet(SUSPENDED, NOT_STARTED)) {
                logger.fine("Resuming " + jobIdString());
                tryStartJob(executionIdSupplier);
            } else {
                logger.info("Not resuming " + jobIdString() + ": not " + SUSPENDED + ", but " + jobStatus.get());
            }
        }
    }

    private boolean hasParticipant(String uuid) {
        return executionPlanMap != null
                && executionPlanMap.keySet().stream().anyMatch(mi -> mi.getUuid().equals(uuid));
    }

    /**
     * Called when job participant is going to gracefully shut down. Will
     * initiate terminal snapshot and when it's done, will complete the
     * returned future.
     *
     * @return a future to wait for or null if there's no need to wait
     */
    @Nullable
    CompletableFuture<Void> onParticipantShutDown(String uuid) {
        synchronized (lock) {
            if (!hasParticipant(uuid)) {
                return null;
            }

            if (jobStatus() == SUSPENDED) {
                return null;
            }

            if (requestedTerminationMode.get() == null) {
                requestTermination(RESTART_GRACEFUL);
            }
            if (requestedTerminationMode.get().isWithTerminalSnapshot()) {
                // this future is null if job is not running, which is ok
                return terminalSnapshotFuture;
            }
            return null; // nothing to wait for
        }
    }

    boolean maybeUpscale(Collection<Member> currentDataMembers) {
        if (!getJobConfig().isAutoScaling()) {
            return false;
        }

        synchronized (lock) {
            // We only compare the number of our participating members and current members.
            // If there is any member in our participants that is not among current data members,
            // this job will be restarted anyway. If it's the other way, then the sizes won't match.
            if (executionPlanMap == null || executionPlanMap.size() == currentDataMembers.size()) {
                LoggingUtil.logFine(logger, "Not up-scaling job %s: already running on all members", jobIdString());
                return false;
            }

            return requestTermination(TerminationMode.RESTART_GRACEFUL);
        }
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

    /**
     * Registered to {@link StartExecutionOperation} invocations to cancel invocations in case of a failure or restart
     */
    private class ExecutionInvocationCallback implements ExecutionCallback<Object> {

        private final AtomicBoolean invocationsCancelled = new AtomicBoolean();
        private final long executionId;

        ExecutionInvocationCallback(long executionId) {
            this.executionId = executionId;
        }

        @Override
        public void onResponse(Object response) {

        }

        @Override
        public void onFailure(Throwable t) {
            cancelInvocations(null);
        }

        void cancelInvocations(TerminationMode mode) {
            if (invocationsCancelled.compareAndSet(false, true)) {
                cancelExecutionInvocations(jobId, executionId, mode);
            }
        }
    }
}
