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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.impl.exception.ShutdownInProgressException;
import com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.COMPLETING;
import static com.hazelcast.jet.core.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_GRACEFUL;
import static com.hazelcast.jet.impl.TerminationMode.TERMINATE_GRACEFUL;
import static com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus.FAILED;
import static com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus.SUCCESSFUL;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_SCAN_PERIOD;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static com.hazelcast.util.executor.ExecutorType.CACHED;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Service to handle MasterContexts on coordinator member. Job-control
 * operations from client are handled here.
 */
public class JobCoordinationService {

    private static final String COORDINATOR_EXECUTOR_NAME = "jet:coordinator";
    private static final long RETRY_DELAY_IN_MILLIS = SECONDS.toMillis(2);

    private final NodeEngineImpl nodeEngine;
    private final JetService jetService;
    private final JetConfig config;
    private final ILogger logger;
    private final JobRepository jobRepository;
    private final SnapshotRepository snapshotRepository;
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();
    private final Set<String> shuttingDownMembers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Object lock = new Object();
    private volatile boolean isShutdown;

    JobCoordinationService(NodeEngineImpl nodeEngine, JetService jetService, JetConfig config,
                           JobRepository jobRepository, SnapshotRepository snapshotRepository) {
        this.nodeEngine = nodeEngine;
        this.jetService = jetService;
        this.config = config;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;
        this.snapshotRepository = snapshotRepository;
    }

    public void init() {
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        HazelcastProperties properties = new HazelcastProperties(config.getProperties());
        long jobScanPeriodInMillis = properties.getMillis(JOB_SCAN_PERIOD);
        executionService.register(COORDINATOR_EXECUTOR_NAME, 2, Integer.MAX_VALUE, CACHED);
        executionService.scheduleWithRepetition(COORDINATOR_EXECUTOR_NAME, this::scanJobs,
                jobScanPeriodInMillis, jobScanPeriodInMillis, MILLISECONDS);
    }

    public void shutdown() {
        synchronized (lock) {
            isShutdown = true;
        }
    }

    void terminateAlJobs() {
        synchronized (lock) {
            masterContexts.forEach((k, v) -> v.requestTermination(TERMINATE_GRACEFUL));
        }
    }

    public void reset() {
        masterContexts.values().forEach(mc -> mc.requestTermination(TerminationMode.CANCEL));
    }

    // only for testing
    public Map<Long, MasterContext> getMasterContexts() {
        return new HashMap<>(masterContexts);
    }

    // only for testing
    public MasterContext getMasterContext(long jobId) {
        return masterContexts.get(jobId);
    }

    /**
     * Scans all job records and updates quorum size of a split-brain protection enabled
     * job with current cluster quorum size if the current cluster quorum size is larger
     */
    void updateQuorumValues() {
        if (!shouldCheckQuorumValues()) {
            return;
        }

        try {
            int currentQuorumSize = getQuorumSize();
            for (JobRecord jobRecord : jobRepository.getJobRecords()) {
                if (jobRecord.getConfig().isSplitBrainProtectionEnabled()) {
                    if (currentQuorumSize > jobRecord.getQuorumSize()) {
                        boolean updated = jobRepository.updateJobQuorumSizeIfLargerThanCurrent(jobRecord.getJobId(),
                                currentQuorumSize);
                        if (updated) {
                            logger.info("Current quorum size: " + jobRecord.getQuorumSize() + " of job "
                                    + idToString(jobRecord.getJobId()) + " is updated to: " + currentQuorumSize);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.fine("check quorum values task failed", e);
        }
    }

    private boolean shouldCheckQuorumValues() {
        return isMaster() && nodeEngine.isRunning()
                && getInternalPartitionService().getPartitionStateManager().isInitialized();
    }

    /**
     * Starts the job if it is not already started or completed. Returns a future
     * which represents result of the job.
     */
    public CompletableFuture<Void> submitOrJoinJob(long jobId, Data dag, JobConfig config) {
        if (!isMaster()) {
            throw new JetException("Cannot submit job " + idToString(jobId) + ". Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        if (isShutdown) {
            throw new ShutdownInProgressException();
        }

        // the order of operations is important.

        // first, check if the job is already completed
        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            logger.fine("Not starting job " + idToString(jobId) + " since already completed with result: " +
                    jobResult);
            return jobResult.asCompletableFuture();
        }

        int quorumSize = config.isSplitBrainProtectionEnabled() ? getQuorumSize() : 0;
        String dagJson = dagToJson(jobId, config, dag);
        JobRecord jobRecord = new JobRecord(jobId, Clock.currentTimeMillis(), dag, dagJson, config, quorumSize, false);
        MasterContext masterContext = new MasterContext(nodeEngine, this, jobRecord);

        synchronized (lock) {
            if (isShutdown) {
                throw new ShutdownInProgressException();
            }

            // just try to initiate the coordination
            MasterContext prev = masterContexts.putIfAbsent(jobId, masterContext);
            if (prev != null) {
                logger.fine("Joining to already started " + prev.jobIdString());
                return prev.completionFuture();
            }
        }

        // If job is not currently running, it might be that it is just completed
        if (completeMasterContextIfJobAlreadyCompleted(masterContext)) {
            return masterContext.completionFuture();
        }

        // If there is no master context and job result at the same time, it means this is the first submission
        jobRepository.putNewJobRecord(jobRecord);

        logger.info("Starting " + masterContext.jobIdString() + " based on submit request from client");
        nodeEngine.getExecutionService().execute(COORDINATOR_EXECUTOR_NAME, () -> tryStartJob(masterContext));

        return masterContext.completionFuture();
    }

    private String dagToJson(long jobId, JobConfig jobConfig, Data dagData) {
        ClassLoader classLoader = jetService.getJobExecutionService().getClassLoader(jobConfig, jobId);
        DAG dag = deserializeWithCustomClassLoader(nodeEngine.getSerializationService(), classLoader, dagData);
        int coopThreadCount = getJetInstance(nodeEngine).getConfig().getInstanceConfig().getCooperativeThreadCount();
        return dag.toJson(coopThreadCount).toString();
    }

    public CompletableFuture<Void> joinSubmittedJob(long jobId) {
        if (!isMaster()) {
            throw new JetException("Cannot join job " + idToString(jobId) + ". Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        if (isShutdown) {
            throw new ShutdownInProgressException();
        }

        JobRecord jobRecord = jobRepository.getJobRecord(jobId);
        if (jobRecord != null) {
            return submitOrJoinJob(jobId, jobRecord.getDag(), jobRecord.getConfig());
        }

        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            return jobResult.asCompletableFuture();
        }

        throw new JobNotFoundException(jobId);
    }

    // Tries to start a job if it is not already running or completed
    private void startJobIfNotStartedOrCompleted(JobRecord jobRecord) {
        // the order of operations is important.
        long jobId = jobRecord.getJobId();
        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            logger.fine("Not starting job " + idToString(jobId) + ", already has result: " + jobResult);
            return;
        }

        MasterContext masterContext;
        synchronized (lock) {
            if (isShutdown) {
                throw new ShutdownInProgressException();
            }

            masterContext = new MasterContext(nodeEngine, this, jobRecord);
            MasterContext prev = masterContexts.putIfAbsent(jobId, masterContext);
            if (prev != null) {
                prev.resumeJob(jobRepository::newExecutionId);
                return;
            }
        }

        // If job is not currently running, it might be that it just completed.
        // Since we've put the MasterContext into the masterContexts map, someone else could
        // have joined to the job in the meantime so we should notify its future.
        if (completeMasterContextIfJobAlreadyCompleted(masterContext)) {
            return;
        }

        logger.info("Starting job " + idToString(masterContext.jobId()) + " discovered by scanning of JobRecords");
        tryStartJob(masterContext);
    }

    // If a job result is present, it completes the master context using the job result
    private boolean completeMasterContextIfJobAlreadyCompleted(MasterContext masterContext) {
        long jobId = masterContext.jobId();
        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            logger.fine("Completing master context for " + masterContext.jobIdString()
                    + " since already completed with result: " + jobResult);
            masterContext.setFinalResult(jobResult.getFailure());
            return masterContexts.remove(jobId, masterContext);
        }

        if (!masterContext.getJobConfig().isAutoRestartOnMemberFailureEnabled()
                && jobRepository.getExecutionIdCount(jobId) > 0) {
            logger.info("Suspending job " + masterContext.jobIdString()
                    + " since auto-restart is disabled and the job has been executed before");
            masterContext.finalizeJob(new TopologyChangedException());
        }

        return false;
    }

    private void tryStartJob(MasterContext masterContext) {
        masterContext.tryStartJob(jobRepository::newExecutionId);
    }

    private int getQuorumSize() {
        return (getDataMemberCount() / 2) + 1;
    }

    boolean isQuorumPresent(int quorumSize) {
        return getDataMemberCount() >= quorumSize;
    }

    private int getDataMemberCount() {
        ClusterService clusterService = nodeEngine.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).size();
    }

    public void terminateJob(long jobId, TerminationMode terminationMode) {
        if (!isMaster()) {
            throw new JetException("Cannot " + terminationMode + " job " + idToString(jobId) + ". Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            throw new IllegalStateException("Cannot " + terminationMode + " job " + idToString(jobId)
                    + " because it already has a result: " + jobResult);
        }

        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            throw new RetryableHazelcastException("No MasterContext found for job " + idToString(jobId)
                    + " for " + terminationMode);
        }

        // User can cancel in any state, other terminations are allowed only when running.
        // This is not technically required (we request termination in any state in case of graceful
        // shutdown), but this method is only called from client. It would be weird for the client to
        // request a restart if the job didn't start yet etc.
        JobStatus jobStatus = masterContext.jobStatus();
        if (jobStatus != RUNNING && terminationMode != CANCEL) {
            throw new IllegalStateException("Cannot " + terminationMode + ", job status is " + jobStatus
                    + ", should be " + RUNNING);
        }

        if (!masterContext.requestTermination(terminationMode)) {
            throw new IllegalStateException("Termination was already requested");
        }
    }

    public Set<Long> getAllJobIds() {
        Set<Long> jobIds = new HashSet<>(jobRepository.getAllJobIds());
        jobIds.addAll(masterContexts.keySet());
        return jobIds;
    }

    /**
     * Returns the job status or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public JobStatus getJobStatus(long jobId) {
        if (!isMaster()) {
            throw new JetException("Cannot query status of job " + idToString(jobId) + ". Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        // first check if there is a job result present.
        // this map is updated first during completion.
        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            return jobResult.getJobStatus();
        }

        // check if there a master context for running job
        MasterContext currentMasterContext = masterContexts.get(jobId);
        if (currentMasterContext != null) {
            JobStatus jobStatus = currentMasterContext.jobStatus();
            if (jobStatus == RUNNING && currentMasterContext.terminationRequested()) {
                jobStatus = COMPLETING;
            }
            return jobStatus;
        }

        // no master context found, job might be just submitted
        JobRecord jobRecord = jobRepository.getJobRecord(jobId);
        if (jobRecord != null) {
            return jobRecord.isSuspended() ? SUSPENDED : NOT_STARTED;
        } else {
            // no job record found, but check job results again
            // since job might have been completed meanwhile.
            jobResult = jobRepository.getJobResult(jobId);
            if (jobResult != null) {
                return jobResult.getJobStatus();
            }
            throw new JobNotFoundException(jobId);
        }
    }

    /**
     * Returns the job submission time or fails with {@link JobNotFoundException}
     * if the requested job is not found.
     */
    public long getJobSubmissionTime(long jobId) {
        if (!isMaster()) {
            throw new JetException("Cannot query submission time of job " + idToString(jobId) + ". Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        JobRecord jobRecord = jobRepository.getJobRecord(jobId);
        if (jobRecord != null) {
            return jobRecord.getCreationTime();
        }

        JobResult jobResult = jobRepository.getJobResult(jobId);
        if (jobResult != null) {
            return jobResult.getCreationTime();
        }

        throw new JobNotFoundException(jobId);
    }

    SnapshotRepository snapshotRepository() {
        return snapshotRepository;
    }

    /**
     * Completes the job which is coordinated with the given master context object.
     */
    void completeJob(MasterContext masterContext, long completionTime, Throwable error) {
        // the order of operations is important.

        long jobId = masterContext.jobId();
        String coordinator = nodeEngine.getNode().getThisUuid();
        jobRepository.completeJob(jobId, coordinator, completionTime, error);
        if (masterContexts.remove(masterContext.jobId(), masterContext)) {
            logger.fine(masterContext.jobIdString() + " is completed");
        } else {
            MasterContext existing = masterContexts.get(jobId);
            if (existing != null) {
                logger.severe("Different master context found to complete " + masterContext.jobIdString()
                        + ", master context execution " + idToString(existing.getExecutionId()));
            } else {
                logger.severe("No master context found to complete " + masterContext.jobIdString());
            }
        }
    }

    void suspendJob(MasterContext masterContext) {
        long jobId = masterContext.jobId();
        jobRepository.updateJobSuspendedStatus(jobId, true);
    }

    public void resumeJob(long jobId) {
        if (jobRepository.updateJobSuspendedStatus(jobId, false)) {
            JobRecord jobRecord = jobRepository.getJobRecord(jobId);
            if (jobRecord != null) {
                startJobIfNotStartedOrCompleted(jobRecord);
            }
        }
    }

    /**
     * Schedules a restart task that will be run in future for the given job
     */
    void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.severe("Master context for job " + idToString(jobId) + " not found to schedule restart");
            return;
        }
        logger.fine("Scheduling restart on master for job " + idToString(jobId));
        nodeEngine.getExecutionService().schedule(COORDINATOR_EXECUTOR_NAME, () -> restartJob(jobId),
                RETRY_DELAY_IN_MILLIS, MILLISECONDS);
    }

    void scheduleSnapshot(long jobId, long executionId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.warning("MasterContext not found to schedule snapshot of " + idToString(jobId));
            return;
        }
        long snapshotInterval = masterContext.getJobConfig().getSnapshotIntervalMillis();
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        if (logger.isFineEnabled()) {
            logger.fine(masterContext.jobIdString() + " snapshot is scheduled in "
                    + snapshotInterval + "ms");
        }
        executionService.schedule(COORDINATOR_EXECUTOR_NAME, () -> beginSnapshot(jobId, executionId),
                snapshotInterval, MILLISECONDS);
    }

    void beginSnapshot(long jobId, long executionId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.warning("MasterContext not found to schedule snapshot of " + idToString(jobId));
            return;
        }
        if (masterContext.completionFuture().isDone() || masterContext.isCancelled()
                || masterContext.jobStatus() != RUNNING) {
            logger.fine("Not starting snapshot since " + masterContext.jobIdString() + " is done.");
            return;
        }

        if (!shouldStartJobs()) {
            scheduleSnapshot(jobId, executionId);
            return;
        }

        masterContext.beginSnapshot(executionId);
    }

    void completeSnapshot(long jobId, long snapshotId, boolean isSuccess, long numBytes, long numKeys, long numChunks) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.warning("MasterContext not found to finalize snapshot of " + idToString(jobId)
                    + " with result: " + isSuccess);
            return;
        }
        try {
            SnapshotStatus status = isSuccess ? SUCCESSFUL : FAILED;
            long elapsed = snapshotRepository.setSnapshotComplete(jobId, snapshotId, status, numBytes, numKeys,
                    numChunks);
            logger.info(String.format("Snapshot %d for %s completed with status %s in %dms, " +
                            "%,d bytes, %,d keys in %,d chunks", snapshotId, masterContext.jobIdString(), status, elapsed,
                            numBytes, numKeys, numChunks));
        } catch (Exception e) {
            logger.warning("Cannot update snapshot status for " + masterContext.jobIdString() + " snapshot "
                    + snapshotId + " isSuccess: " + isSuccess);
            return;
        }
        try {
            if (isSuccess) {
                snapshotRepository.deleteAllSnapshotsExceptOne(jobId, snapshotId);
            } else {
                snapshotRepository.deleteSingleSnapshot(jobId, snapshotId);
            }
        } catch (Exception e) {
            logger.warning("Cannot delete old snapshots for " + masterContext.jobIdString());
        }
    }

    boolean shouldStartJobs() {
        if (!isMaster() || !nodeEngine.isRunning()) {
            return false;
        }

        if (nodeEngine.getClusterService().getMembers().stream()
                      .anyMatch(m -> shuttingDownMembers.contains(m.getUuid()))) {
            return false;
        }

        InternalPartitionServiceImpl partitionService = getInternalPartitionService();
        return partitionService.getPartitionStateManager().isInitialized()
                && partitionService.isMigrationAllowed()
                && !partitionService.hasOnGoingMigrationLocal();
    }

    /**
     * Return the job IDs of jobs with given name, sorted by creation time, newest first.
     */
    public List<Long> getJobIds(String name) {
        Map<Long, Long> jobs = new HashMap<>();

        jobRepository.getJobRecords(name).forEach(r -> jobs.put(r.getJobId(), r.getCreationTime()));

        masterContexts.values().stream()
                      .filter(ctx -> name.equals(ctx.getJobConfig().getName()))
                      .forEach(ctx -> jobs.put(ctx.jobId(), ctx.getJobRecord().getCreationTime()));

        jobRepository.getJobResults(name)
                  .forEach(r -> jobs.put(r.getJobId(), r.getCreationTime()));

        return jobs.entrySet().stream()
                   .sorted(comparing((Function<Entry<Long, Long>, Long>) Entry::getValue).reversed())
                   .map(Entry::getKey).collect(toList());
    }

    private InternalPartitionServiceImpl getInternalPartitionService() {
        Node node = nodeEngine.getNode();
        return (InternalPartitionServiceImpl) node.getPartitionService();
    }

    /**
     * Restarts a job for a new execution if the cluster is stable.
     * Otherwise, it reschedules the restart task.
     */
    void restartJob(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext == null) {
            logger.severe("Master context for job " + idToString(jobId) + " not found to restart");
            return;
        }
        tryStartJob(masterContext);
    }

    // runs periodically to restart jobs on coordinator failure and perform gc
    private void scanJobs() {
        if (!shouldStartJobs()) {
            return;
        }

        try {
            Collection<JobRecord> jobs = jobRepository.getJobRecords();
            jobs.stream()
                .filter(jobRecord -> !jobRecord.isSuspended())
                .forEach(this::startJobIfNotStartedOrCompleted);

            performCleanup();
        } catch (Exception e) {
            if (e instanceof HazelcastInstanceNotActiveException) {
                return;
            }

            logger.severe("Scanning jobs failed", e);
        }
    }

    private void performCleanup() {
        Set<Long> runningJobIds = masterContexts.keySet();
        jobRepository.cleanup(runningJobIds);
    }

    private boolean isMaster() {
        return nodeEngine.getClusterService().isMaster();
    }

    JetService getJetService() {
        return jetService;
    }

    public void addShuttingDownMember(String uuid) {
        shuttingDownMembers.add(uuid);
        masterContexts.values().stream()
                      .filter(mc -> mc.hasParticipant(uuid))
                      .forEach(mc -> mc.requestTermination(RESTART_GRACEFUL));
    }

    Set<String> getShuttingDownMembers() {
        return shuttingDownMembers;
    }

    void onMemberLeave(String uuid) {
        shuttingDownMembers.remove(uuid);
    }
}
