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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JobStatus;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.jet.config.JetConfig.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_SCAN_PERIOD;
import static com.hazelcast.jet.impl.util.Util.formatIds;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.util.executor.ExecutorType.CACHED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JobCoordinationService {

    private static final String COORDINATOR_EXECUTOR_NAME = "jet:coordinator";
    private static final long RETRY_DELAY_IN_MILLIS = SECONDS.toMillis(2);
    private static final long LOCK_ACQUIRE_ATTEMPT_TIMEOUT_IN_MILLIS = SECONDS.toMillis(1);

    private final NodeEngineImpl nodeEngine;
    private final JetConfig config;
    private final ILogger logger;
    private final JobRepository jobRepository;
    private final JobExecutionService jobExecutionService;
    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();
    private final IMap<Long, JobResult> jobResults;

    public JobCoordinationService(NodeEngineImpl nodeEngine, JetConfig config,
                                  JobRepository jobRepository, JobExecutionService jobExecutionService) {
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;
        this.jobExecutionService = jobExecutionService;
        this.jobResults = nodeEngine.getHazelcastInstance().getMap(JOB_RESULTS_MAP_NAME);
    }

    public void init() {
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        HazelcastProperties properties = new HazelcastProperties(config.getProperties());
        long jobScanPeriodInMillis = properties.getMillis(JOB_SCAN_PERIOD);
        executionService.register(COORDINATOR_EXECUTOR_NAME, 2, Integer.MAX_VALUE, CACHED);
        executionService.scheduleWithRepetition(COORDINATOR_EXECUTOR_NAME, this::scanJobs,
                jobScanPeriodInMillis, jobScanPeriodInMillis, MILLISECONDS);
    }

    public void reset() {
        masterContexts.clear();
    }

    public ClassLoader getClassLoader(long jobId) {
        PrivilegedAction<JetClassLoader> action = () -> new JetClassLoader(jobId, jobRepository.getJobResources(jobId));
        return jobExecutionService.getClassLoader(jobId, action);
    }

    // visible only for testing
    public Map<Long, MasterContext> getMasterContexts() {
        return new HashMap<>(masterContexts);
    }

    // visible only for testing
    public MasterContext getMasterContext(long jobId) {
        return masterContexts.get(jobId);
    }

    public Map<MemberInfo, ExecutionPlan> createExecutionPlans(MembersView membersView, DAG dag) {
        return ExecutionPlanBuilder.createExecutionPlans(nodeEngine, membersView, dag,
                config.getInstanceConfig().getCooperativeThreadCount());
    }

    public CompletableFuture<Boolean> startOrJoinJob(long jobId, Data dag, JobConfig config) {
        if (!isMaster()) {
            throw new JetException("Job cannot be started here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        // if we receive start job call but cannot find the resource marker there, it means either
        // job is completed or resources are lost because of failures or there is an ongoing split-brain merge
        if (!jobRepository.isResourceUploadCompleted(jobId)) {
            CompletableFuture<Boolean> future = getCompletedJobFutureIfPresent(jobId);
            if (future != null) {
                return future;
            }

            throw new RetryableHazelcastException();
        }

        if (!tryLock()) {
            throw new RetryableHazelcastException();
        }

        MasterContext masterContext;
        try {
            CompletableFuture<Boolean> future = getCompletedJobFutureIfPresent(jobId);
            if (future != null) {
                return future;
            }

            int quorumSize = config.isSplitBrainProtectionEnabled() ? getQuorumSize() : 0;
            JobRecord jobRecord = new JobRecord(jobId, dag, config, quorumSize);
            jobRepository.putNewJobRecord(jobRecord);

            masterContext = new MasterContext(nodeEngine, this, jobRecord);
            MasterContext prev = masterContexts.putIfAbsent(jobId, masterContext);
            if (prev != null) {
                logger.fine("Joining to already started job " + idToString(jobId));
                return prev.completionFuture();
            }
        } finally {
            lock.unlock();
        }

        logger.info("Starting new job " + idToString(jobId));
        masterContext.tryStartJob(jobRepository::newId);
        return masterContext.completionFuture();
    }

    private CompletableFuture<Boolean> getCompletedJobFutureIfPresent(long jobId) {
        JobResult jobResult = jobResults.get(jobId);
        if (jobResult != null) {
            logger.fine("Not starting job " + idToString(jobId) + " since already completed with result: " +
                    jobResult);
            return jobResult.asCompletableFuture();
        }
        return null;
    }

    // called when the lock is acquired
    private MasterContext createMasterContextIfJobNotStarted(JobRecord jobRecord) {
        if (!isMaster()) {
            throw new JetException("Job cannot be started here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        long jobId = jobRecord.getJobId();
        if (jobResults.get(jobId) != null || masterContexts.containsKey(jobId)) {
            return null;
        }

        MasterContext masterContext = new MasterContext(nodeEngine, this, jobRecord);
        masterContexts.put(jobId, masterContext);

        return masterContext;
    }

    private boolean tryLock() {
        try {
            return lock.tryLock(LOCK_ACQUIRE_ATTEMPT_TIMEOUT_IN_MILLIS, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private int getQuorumSize() {
        int dataMemberCount = getDataMemberCount();
        return (dataMemberCount / 2) + 1;
    }

    boolean isQuorumPresent(int quorumSize) {
        return getDataMemberCount() >= quorumSize;
    }

    private int getDataMemberCount() {
        ClusterService clusterService = nodeEngine.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).size();
    }

    public JobStatus getJobStatus(long jobId) {
        if (!isMaster()) {
            throw new JetException("Job status cannot be queried here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        MasterContext currentMasterContext = masterContexts.get(jobId);
        if (currentMasterContext != null) {
            return currentMasterContext.jobStatus();
        }

        JobRecord jobRecord = jobRepository.getJob(jobId);
        if (jobRecord == null) {
            JobResult jobResult = jobResults.get(jobId);
            if (jobResult != null) {
                return jobResult.isSuccessfulOrCancelled() ? JobStatus.COMPLETED : JobStatus.FAILED;
            } else {
                throw new IllegalStateException("Job " + idToString(jobId) + " not found");
            }
        } else {
            return JobStatus.NOT_STARTED;
        }
    }

    void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            logger.fine("Scheduling master context restart for job " + idToString(jobId));
            nodeEngine.getExecutionService().schedule(COORDINATOR_EXECUTOR_NAME, () -> restartJob(jobId),
                    RETRY_DELAY_IN_MILLIS, MILLISECONDS);
        } else {
            logger.severe("Master context for job " + idToString(jobId) + " not found to schedule restart");
        }
    }

    CompletableFuture<Boolean> completeJob(MasterContext masterContext, long completionTime, Throwable error) {
        CompletableFuture<Boolean> callback = new CompletableFuture<>();
        completeJob(masterContext, completionTime, error, callback);
        return callback;
    }

    private void completeJob(MasterContext masterContext, long completionTime,
                             Throwable error, CompletableFuture<Boolean> future) {
        if (!tryLock()) {
            logger.fine("Complete of job " + idToString(masterContext.getJobId()) + " is rescheduled.");
            InternalExecutionService executionService = nodeEngine.getExecutionService();
            executionService.schedule(COORDINATOR_EXECUTOR_NAME,
                    () -> completeJob(masterContext, completionTime, error, future),
                    RETRY_DELAY_IN_MILLIS, MILLISECONDS);
            return;
        }

        try {
            long jobId = masterContext.getJobId();
            long executionId = masterContext.getExecutionId();
            if (masterContexts.remove(masterContext.getJobId(), masterContext)) {
                long jobCreationTime = jobRepository.getJobCreationTime(jobId);
                String coordinator = nodeEngine.getNode().getThisUuid();
                JobResult jobResult = new JobResult(jobId, coordinator, jobCreationTime, completionTime, error);
                JobResult prev = jobResults.putIfAbsent(jobId, jobResult);
                if (prev != null) {
                    throw new IllegalStateException(jobResult + " already exists in the " + JOB_RESULTS_MAP_NAME
                            + " map");
                }
                jobRepository.deleteJob(jobId);
                logger.fine(formatIds(jobId, executionId) + " is completed");
            } else {
                MasterContext existing = masterContexts.get(jobId);
                if (existing != null) {
                    logger.severe("Different master context found to complete " + formatIds(jobId, executionId)
                            + ", master context execution " + idToString(existing.getExecutionId()));
                } else {
                    logger.severe("No master context found to complete " + formatIds(jobId, executionId));
                }
            }
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            return;
        } finally {
            lock.unlock();
        }

        future.complete(true);
    }

    private void restartJob(long jobId) {
        if (!shouldStartJobs()) {
            scheduleRestart(jobId);
            return;
        }

        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            masterContext.tryStartJob(jobRepository::newId);
        } else {
            logger.severe("Master context for job " + idToString(jobId) + " not found to restart");
        }
    }

    @SuppressFBWarnings(value = "UC_USELESS_OBJECT", justification = "false positive findbugs warning")
    private void scanJobs() {
        if (!shouldStartJobs() || !tryLock()) {
            return;
        }

        List<MasterContext> masterContextsToStart = new ArrayList<>();
        try {
            cleanupExpiredJobs();

            Collection<JobRecord> jobs = jobRepository.getJobRecords();
            if (jobs.isEmpty()) {
                return;
            }

            for (JobRecord job : jobs) {
                MasterContext masterContext = createMasterContextIfJobNotStarted(job);
                if (masterContext != null) {
                    masterContextsToStart.add(masterContext);
                }
            }
        } catch (Exception e) {
            if (e instanceof HazelcastInstanceNotActiveException) {
                return;
            }
            logger.severe("Scanning jobs failed", e);
        } finally {
            lock.unlock();
        }

        masterContextsToStart.forEach(masterContext -> {
            logger.info("Starting new job " + idToString(masterContext.getJobId()));
            masterContext.tryStartJob(jobRepository::newId);
        });
    }

    private boolean shouldStartJobs() {
        if (!(isMaster() && nodeEngine.isRunning())) {
            return false;
        }

        Node node = nodeEngine.getNode();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getPartitionStateManager().isInitialized()
                && partitionService.isMigrationAllowed()
                && !partitionService.hasOnGoingMigrationLocal();
    }

    private void cleanupExpiredJobs() {
        Set<Long> completedJobIds = jobResults.keySet();
        Set<Long> runningJobIds = masterContexts.keySet();
        jobRepository.cleanup(completedJobIds, runningJobIds);
    }

    // visible for testing
    public JobResult getResult(long jobId) {
        return jobResults.get(jobId);
    }

    private boolean isMaster() {
        return nodeEngine.getClusterService().isMaster();
    }

}
