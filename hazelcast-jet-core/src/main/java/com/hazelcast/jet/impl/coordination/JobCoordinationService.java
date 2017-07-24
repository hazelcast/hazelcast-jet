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
import com.hazelcast.nio.Address;
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

    private static final long JOB_SCANNER_TASK_PERIOD_IN_MILLIS = TimeUnit.SECONDS.toMillis(1);

    private final NodeEngineImpl nodeEngine;
    private final JetConfig config;
    private final ILogger logger;
    private final JobRepository jobRepository;
    private final JobResultRepository jobResultRepository;

    private final Object lock = new Object();
    private final ConcurrentMap<Long, MasterContext> masterContexts = new ConcurrentHashMap<>();

    public JobCoordinationService(NodeEngineImpl nodeEngine, JetConfig config,
                                  JobRepository jobRepository, JobResultRepository jobResultRepository) {
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;
        this.jobResultRepository = jobResultRepository;
    }

    public void init() {
        InternalExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(COORDINATOR_EXECUTOR_NAME, 2, Integer.MAX_VALUE, CACHED);
        executionService.scheduleWithRepetition(COORDINATOR_EXECUTOR_NAME, this::scanJobs,
                0, JOB_SCANNER_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    public Map<Long, MasterContext> getMasterContexts() {
        return new HashMap<>(masterContexts);
    }

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
            JobResult jobResult = jobResultRepository.getJobResult(jobId);
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

    long generateRandomId() {
        return jobRepository.newJobId();
    }

    void scheduleRestart(long jobId) {
        MasterContext masterContext = masterContexts.get(jobId);
        if (masterContext != null) {
            logger.fine("Scheduling master context restart for job " + jobId);
            nodeEngine.getExecutionService().schedule(COORDINATOR_EXECUTOR_NAME, () -> restartJob(jobId),
                    JOB_SCANNER_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        } else {
            logger.severe("Master context for job " + jobId + " not found to schedule restart" );
        }
    }

    void completeJob(MasterContext masterContext, long completionTime, Throwable error) {
        synchronized (lock) {
            long jobId = masterContext.getJobId(), executionId = masterContext.getExecutionId();
            try {
                if (masterContexts.remove(masterContext.getJobId(), masterContext)) {
                    long jobCreationTime = jobRepository.getJobCreationTime(jobId);
                    Address coordinator = nodeEngine.getThisAddress();
                    JobResult jobResult = new JobResult(jobId, coordinator, jobCreationTime, completionTime, error);
                    jobResultRepository.completeJob(jobResult);

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
            logger.severe("Master context for job " + jobId + " not found to restart" );
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

}
