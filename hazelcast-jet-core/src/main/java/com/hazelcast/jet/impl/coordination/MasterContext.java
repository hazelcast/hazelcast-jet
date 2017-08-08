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

package com.hazelcast.jet.impl.coordination;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JobStatus;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CompleteOperation;
import com.hazelcast.jet.impl.operation.ExecuteOperation;
import com.hazelcast.jet.impl.operation.InitOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.MaxByAggregator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.JobStatus.COMPLETED;
import static com.hazelcast.jet.JobStatus.FAILED;
import static com.hazelcast.jet.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.JobStatus.RESTARTING;
import static com.hazelcast.jet.JobStatus.RUNNING;
import static com.hazelcast.jet.JobStatus.STARTING;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isJobRestartRequired;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.Util.formatIds;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class MasterContext {

    private static final String SNAPSHOTS_MAP_NAME = "__jet.jobs.snapshots";

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final JobRecord jobRecord;
    private final long jobId;
    private final CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();
    private final AtomicReference<JobStatus> jobStatus = new AtomicReference<>(NOT_STARTED);

    private volatile long nextSnapshotId;

    private volatile long executionId;
    private volatile long jobStartTime;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;

    private volatile ScheduledFuture<?> scheduledSnapshotFuture;

    private volatile List<String> snapshottableVertices;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, JobRecord jobRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
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

    CompletableFuture<Boolean> completionFuture() {
        return completionFuture;
    }

    public boolean cancel() {
        return completionFuture.cancel(true);
    }

    public boolean isCancelled() {
        return completionFuture.isCancelled();
    }

    public JobStatus jobStatus() {
        return jobStatus.get();
    }

    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        if (!setJobStatusToStarting()) {
            return;
        }

        if (scheduleRestartIfQuorumAbsent()) {
            return;
        }

        executionId = executionIdSupplier.apply(jobId);
        MembersView membersView = getMembersView();
        try {
            executionPlanMap = createExecutionPlans(membersView);
        } catch (TopologyChangedException e) {
            logger.severe("Execution plans could not be created for " + formatIds(jobId, executionId), e);
            scheduleRestart();
            return;
        }

        long snapshotId = findUsableSnapshot();

        logger.fine("Built execution plans for " + formatIds(jobId, executionId));
        Set<MemberInfo> participants = executionPlanMap.keySet();
        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitOperation(jobId, executionId, membersView.getVersion(), participants, plan, snapshotId);
        invoke(operationCtor, this::onInitStepCompleted, null);
    }

    /**
     * Return snapshotId of newest complete snapshot for this job.
     */
    private long findUsableSnapshot() {
        IMap<Long, SnapshotRecord> snapshotsMap = nodeEngine.getHazelcastInstance().getMap(SNAPSHOTS_MAP_NAME);
        Entry<Long, SnapshotRecord> newestSnapshot =
                snapshotsMap.aggregate(new MaxByAggregator<>("creationTime"), Predicates.equal("jobId", jobId));
        return newestSnapshot != null ? newestSnapshot.getKey() : 0;
    }

    /**
     * Set jobStatus to starting. Return false if job is already in started state.
     */
    private boolean setJobStatusToStarting() {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            logger.severe("Cannot init job " + idToString(jobId) + ": it is already " + status);
            return false;
        }

        if (completionFuture.isCancelled()) {
            logger.fine("Skipping init job " + idToString(jobId) + ": is already cancelled.");
            onCompleteStepCompleted(null);
            return false;
        }

        if (status == NOT_STARTED) {
            if (!jobStatus.compareAndSet(NOT_STARTED, STARTING)) {
                logger.fine("Cannot init job " + idToString(jobId) + ": someone else is just starting it");
                return false;
            }

            jobStartTime = System.currentTimeMillis();
        } else {
            jobStatus.compareAndSet(RUNNING, RESTARTING);
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

        logger.fine("Rescheduling job " + idToString(jobId) + " restart since quorum size " + quorumSize
                + " is not met");
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

    private Map<MemberInfo, ExecutionPlan> createExecutionPlans(MembersView membersView) {
        DAG dag = deserializeDAG();

        logger.info("Start executing " + formatIds(jobId, executionId) + ", status " + jobStatus()
                + ": " + dag);
        logger.fine("Building execution plan for " + formatIds(jobId, executionId));
        return coordinationService.createExecutionPlans(membersView, dag, jobRecord.getConfig());
    }

    private DAG deserializeDAG() {
        ClassLoader cl = coordinationService.getClassLoader(jobId);
        return deserializeWithCustomClassLoader(nodeEngine.getSerializationService(), cl, jobRecord.getDag());
    }

    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getInitResult(responses);

        if (error == null) {
            JobStatus status = jobStatus();

            if (!(status == STARTING || status == RESTARTING)) {
                error = new IllegalStateException("Cannot execute " + formatIds(jobId, executionId)
                        + ": status is " + status);
            }
        }

        if (error == null) {
            invokeExecute();
        } else {
            invokeComplete(error);
        }
    }

    private Throwable getInitResult(Map<MemberInfo, Object> responses) {
        if (completionFuture.isCancelled()) {
            logger.fine(formatIds(jobId, executionId) + " to be cancelled after init");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Init of " + formatIds(jobId, executionId) + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Init of " + formatIds(jobId, executionId) + " failed with: " + failures);

        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !isJobRestartRequiredFailure(t))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private Map<Boolean, List<Entry<MemberInfo, Object>>> groupResponses(Map<MemberInfo, Object> responses) {
        return responses
                .entrySet()
                .stream()
                .collect(partitioningBy(e -> e.getValue() instanceof Throwable));
    }

    private boolean isJobRestartRequiredFailure(Object response) {
        return response instanceof Throwable && isJobRestartRequired((Throwable) response);
    }

    private void invokeExecute() {
        jobStatus.set(RUNNING);
        logger.fine("Executing " + formatIds(jobId, executionId));
        Function<ExecutionPlan, Operation> operationCtor = plan -> new ExecuteOperation(jobId, executionId);
        invoke(operationCtor, this::onExecuteStepCompleted, completionFuture);
        scheduleSnapshots();
    }

    private void scheduleSnapshots() {
        long interval = jobRecord.getConfig().getSnapshotInterval();
        if (interval <= 0) {
            return;
        }

        snapshottableVertices = executionPlanMap.entrySet().stream().filter(e -> e.getKey().getAddress().equals
                (nodeEngine.getThisAddress()))
                                                .map(e -> e.getValue().snapshottableVertices()).findFirst().orElseThrow(
                        () -> new IllegalStateException("Could not find master node within execution plan map"));

        String name = "jet.snapshot." + jobId + "." + executionId;
        scheduledSnapshotFuture = nodeEngine.getExecutionService().scheduleWithRepetition(
                name, this::takeSnapshot, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void takeSnapshot() {
        IMap<Object, Object> snapshotsMap = getSnapshotsMap();
        SnapshotRecord record = new SnapshotRecord(jobId, nextSnapshotId++, snapshottableVertices);
        if (snapshotsMap.putIfAbsent(Arrays.asList(jobId,  record.snapshotId()), record) != null) {
            logger.severe("Snapshot with id " + record.snapshotId() + " already exists for job " + idToString(jobId));
            //TODO: should job be failed here?
            return;
        }

        logger.info(String.format("Starting snapshot %s for job %s", record.snapshotId(), idToString(jobId)));
        Function<ExecutionPlan, Operation> factory = plan -> new SnapshotOperation(jobId, executionId, record.snapshotId());
        invoke(factory, responses -> onSnapshotCompleted(responses, record.snapshotId()), completionFuture);
    }

    private void onSnapshotCompleted(Map<MemberInfo, Object> responses, long snapshotId) {
        // check if all members were successful
        for (Object r : responses.values()) {
            if (r instanceof Throwable) {
                logger.warning(SnapshotOperation.class.getSimpleName() + " for " + formatIds(jobId, executionId)
                        + " failed on some members: " + r, (Throwable) r);
                // fail the job
                completionFuture.completeExceptionally((Throwable) r);
            }
        }

        // mark the record in the map as completed
        IMap<Object, Object> map = getSnapshotsMap();
        long creationTime = (long) map.executeOnKey(Arrays.asList(jobId, snapshotId),
                new MarkRecordCompleteEntryProcessor());

        logger.info(String.format("Snapshot %s for job %s completed in %dms", snapshotId,
                idToString(jobId), System.currentTimeMillis() - creationTime));

        // TODO delete older snapshots
    }

    private IMap<Object, Object> getSnapshotsMap() {
        return nodeEngine.getHazelcastInstance().getMap(SNAPSHOTS_MAP_NAME);
    }

    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        if (scheduledSnapshotFuture != null) {
            scheduledSnapshotFuture.cancel(true);
        }
        invokeComplete(getExecuteResult(responses));
    }

    private Throwable getExecuteResult(Map<MemberInfo, Object> responses) {
        if (completionFuture.isCancelled()) {
            logger.fine(formatIds(jobId, executionId) + " to be cancelled after execute");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Execute of " + formatIds(jobId, executionId) + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Execute of " + formatIds(jobId, executionId) + " has failures: " + failures);

        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException || isJobRestartRequiredFailure(t)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private void invokeComplete(Throwable error) {
        JobStatus status = jobStatus();

        Throwable finalError;
        if (status == STARTING || status == RESTARTING || status == RUNNING) {
            logger.fine("Completing " + formatIds(jobId, executionId));
            finalError = error;
        } else {
            if (error != null) {
                logger.severe("Cannot properly complete failed " + formatIds(jobId, executionId)
                        + ": status is " + status, error);
            } else {
                logger.severe("Cannot properly complete " + formatIds(jobId, executionId)
                        + ": status is " + status);
            }

            finalError = new IllegalStateException("Job coordination failed.");
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new CompleteOperation(executionId, finalError);
        invoke(operationCtor, responses -> onCompleteStepCompleted(error), null);
    }

    private void onCompleteStepCompleted(@Nullable Throwable failure) {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            if (failure != null) {
                logger.severe("Ignoring failure completion of " + idToString(jobId) + " because status is "
                        + status, failure);
            } else {
                logger.severe("Ignoring completion of " + idToString(jobId) + " because status is " + status);
            }

            return;
        }

        long completionTime = System.currentTimeMillis();

        if (failure instanceof TopologyChangedException) {
            scheduleRestart();
            return;
        }

        long elapsed = completionTime - jobStartTime;

        if (isSuccess(failure)) {
            logger.info("Execution of " + formatIds(jobId, executionId) + " completed in " + elapsed + " ms");
        } else {
            logger.warning("Execution of " + formatIds(jobId, executionId)
                    + " failed in " + elapsed + " ms", failure);
        }

        try {
            coordinationService.completeJob(this, completionTime, failure);
        } catch (RuntimeException e) {
            logger.warning("Completion of " + formatIds(jobId, executionId)
                    + " failed in " + elapsed + " ms", failure);
        } finally {
            setFinalResult(failure);
        }
    }

    void setFinalResult(Throwable failure) {
        JobStatus status = isSuccess(failure) ? COMPLETED : FAILED;
        jobStatus.set(status);
        if (status == COMPLETED) {
            completionFuture.complete(true);
        } else {
            completionFuture.completeExceptionally(failure);
        }
    }

    private boolean isSuccess(Throwable failure) {
        return (failure == null || failure instanceof CancellationException);
    }

    private void invoke(Function<ExecutionPlan, Operation> operationCtor,
                        Consumer<Map<MemberInfo, Object>> completionCallback,
                        CompletableFuture cancellation) {
        CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        Map<MemberInfo, InternalCompletableFuture<Object>> futures = new ConcurrentHashMap<>();
        invokeOnParticipants(futures, doneFuture, operationCtor);

        // once all invocations return, notify the completion callback
        doneFuture.whenComplete((aVoid, throwable) -> {
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
        });

        boolean cancelOnFailure = (cancellation != null);

        // if cancel on failure is true, we should cancel invocations when the given future is cancelled, or
        // any of the invocations fail

        if (cancelOnFailure) {
            cancellation.whenComplete((r, e) -> {
                if (e instanceof CancellationException) {
                    futures.values().forEach(f -> f.cancel(true));
                }
            });

            ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                }

                @Override
                public void onFailure(Throwable t) {
                    futures.values().forEach(f -> f.cancel(true));
                }
            };

            futures.values().forEach(f -> f.andThen(callback));
        }
    }

    private void invokeOnParticipants(Map<MemberInfo, InternalCompletableFuture<Object>> futures,
                                      CompletableFuture<Void> doneFuture,
                                      Function<ExecutionPlan, Operation> opCtor) {
        AtomicInteger doneLatch = new AtomicInteger(executionPlanMap.size());

        for (Entry<MemberInfo, ExecutionPlan> e : executionPlanMap.entrySet()) {
            MemberInfo member = e.getKey();
            Operation op = opCtor.apply(e.getValue());
            InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                         .createInvocationBuilder(JetService.SERVICE_NAME, op, member.getAddress())
                         .setDoneCallback(() -> {
                             if (doneLatch.decrementAndGet() == 0) {
                                 doneFuture.complete(null);
                             }
                         })
                         .invoke();
            futures.put(member, future);
        }
    }

    private static class MarkRecordCompleteEntryProcessor extends AbstractEntryProcessor<Object, SnapshotRecord> {
        @Override
        public Object process(Entry<Object, SnapshotRecord> entry) {
            SnapshotRecord record = entry.getValue();
            record.complete();
            entry.setValue(record);
            return record.creationTime();
        }
    }
}
