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

import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.executor.ExecutorType.CACHED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JobCoordinationService {

    private static final String COORDINATOR_EXECUTOR_NAME = "jet:coordinator";
    private static final String JOB_RESULTS_MAP_NAME = "__jet.jobs.results";

    private static final long JOB_SCANNER_TASK_PERIOD_IN_MILLIS = TimeUnit.SECONDS.toMillis(1);

    private final NodeEngineImpl nodeEngine;
    private final JetConfig config;
    private final ILogger logger;
    private final JobRepository jobRepository;
    private final Object lock = new Object();
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();
    private final IMap<Long, JobResult> jobResults;

    public JobCoordinationService(NodeEngineImpl nodeEngine, JetConfig config,
                                  JobRepository jobRepository) {
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;
        this.jobResults = nodeEngine.getHazelcastInstance().getMap(JOB_RESULTS_MAP_NAME);
    }

    public void init() {
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(COORDINATOR_EXECUTOR_NAME, 2, Integer.MAX_VALUE, CACHED);
        executionService.scheduleWithRepetition(COORDINATOR_EXECUTOR_NAME, this::scanJobs,
                0, JOB_SCANNER_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
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

    public CompletableFuture<Throwable> startOrJoinJob(long jobId) {
        if (!nodeEngine.getClusterService().isMaster()) {
            throw new JetException("Job cannot be started here. Master address: "
                    + nodeEngine.getClusterService().getMasterAddress());
        }

        MasterContext newMasterContext;
        synchronized (lock) {
            JobResult jobResult = jobResults.get(jobId);
            if (jobResult != null) {
                logger.fine("Not starting job " + jobId + " since already completed -> " + jobResult);
                return jobResult.asCompletableFuture();
            }

            JobRecord jobRecord = jobRepository.getJob(jobId);
            if (jobRecord == null) {
                throw new IllegalStateException("Job " + jobId + " not found");
            }

            MasterContext currentMasterContext = masterContexts.get(jobId);
            if (currentMasterContext != null) {
                return currentMasterContext.getCompletionFuture();
            }

            newMasterContext = new MasterContext(nodeEngine, this, jobId, jobRecord.getDag());
            masterContexts.put(jobId, newMasterContext);

            logger.info("Starting new job " + jobId);
        }

        return newMasterContext.start();
    }

    long newId() {
        return jobRepository.newId();
    }

    void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            logger.fine("Scheduling master context restart for job " + jobId);
            nodeEngine.getExecutionService().schedule(COORDINATOR_EXECUTOR_NAME, () -> restartJob(jobId),
                    JOB_SCANNER_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        } else {
            logger.severe("Master context for job " + jobId + " not found to schedule restart");
        }
    }

    void completeJob(MasterContext masterContext, long completionTime, Throwable error) {
        synchronized (lock) {
            long jobId = masterContext.getJobId();
            long executionId = masterContext.getExecutionId();
            try {
                if (masterContexts.remove(masterContext.getJobId(), masterContext)) {
                    long jobCreationTime = jobRepository.getJobCreationTime(jobId);
                    String coordinator = nodeEngine.getNode().getThisUuid();
                    JobResult jobResult = new JobResult(jobId, coordinator, jobCreationTime, completionTime, error);
                    JobResult prev = jobResults.putIfAbsent(jobId, jobResult);
                    if (prev != null) {
                        throw new IllegalStateException(jobResult + " already exists in the job record results map!");
                    }
                    jobRepository.deleteJob(jobId);
                    logger.fine("Job " + jobId + " execution " + executionId + " is completed.");
                } else {
                    MasterContext existing = masterContexts.get(jobId);
                    if (existing != null) {
                        logger.severe("Different master context found to complete job " + jobId
                                + " execution " + executionId + " master context execution " + existing.getExecutionId());
                    } else {
                        logger.severe("No master context found to complete job " + jobId + " execution " + executionId);
                    }
                }
            } catch (Exception e) {
                logger.severe("Completion of job " + jobId + " execution " + executionId + " is failed.", e);
            }
        }
    }

    private void restartJob(long jobId) {
        if (!shouldStartJobs()) {
            scheduleRestart(jobId);
            return;
        }

        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            masterContext.start();
        } else {
            logger.severe("Master context for job " + jobId + " not found to restart");
        }
    }

    private void scanJobs() {
        if (!shouldStartJobs()) {
            return;
        }

        IMap<Long, JobRecord> jobs = jobRepository.getJobs();
        if (jobs.isEmpty()) {
            return;
        }

        try {
            jobs.keySet().forEach(this::startOrJoinJob);
        } catch (Exception e) {
            logger.severe("Scanning jobs is failed", e);
        }
    }

    private boolean shouldStartJobs() {
        Node node = nodeEngine.getNode();
        if (!(node.isMaster() && node.isRunning())) {
            return false;
        }

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();

        return partitionService.getPartitionStateManager().isInitialized()
                && partitionService.isMigrationAllowed()
                && !partitionService.hasOnGoingMigrationLocal();
    }

    // visible for testing
    public JobResult getResult(long jobId) {
        return jobResults.get(jobId);
    }
}
