package com.hazelcast.jet.impl.coordination;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.JobResult.JobResultKey;
import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class JobResultRepository {

    private static final String JOB_RESULTS_MAP_NAME = "__jet_job_results";

    private final HazelcastInstance instance;

    private final Node node;

    private final ILogger logger;

    private final JobRepository jobRepository;

    public JobResultRepository(NodeEngineImpl nodeEngine, JobRepository jobRepository) {
        this.instance = nodeEngine.getHazelcastInstance();
        this.node = nodeEngine.getNode();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRepository = jobRepository;
    }

    public JobResult getJobResult(long jobId) {
        IMap<JobResultKey, JobResult> jobResultsMap = getJobResultsMap();

        JobResultKey key = new JobResultKey(jobId, node.getMasterAddress());
        JobResult jobResult = jobResultsMap.get(key);

        if (jobResult != null) {
            return jobResult;
        }

        ClusterServiceImpl clusterService = node.getClusterService();
        List<Address> memberAddresses = clusterService.getMembers().stream().map(Member::getAddress).collect(toList());

        Collection<JobResult> jobResults = jobResultsMap.values(new NonMemberCoordinatorPredicate(memberAddresses));

        if (jobResults.isEmpty()) {
            return null;
        }

        if (jobResults.size() > 1) {
            logger.info(jobResults.size() + " job result records are found for job: "
                    + jobId + " -> " + jobResults);
        }

        return jobResults.iterator().next();
    }

    private IMap<JobResultKey, JobResult> getJobResultsMap() {
        return instance.getMap(JOB_RESULTS_MAP_NAME);
    }

    public void completeJob(JobResult jobResult) {
        if (!(node.isMaster() && node.getThisAddress().equals(jobResult.getCoordinator()))) {
            throw new IllegalStateException("Cannot persist " + jobResult);
        }

        long jobId = jobResult.getJobId();

        IMap<JobResultKey, JobResult> jobResultsMap = getJobResultsMap();
        JobResult curr = jobResultsMap.putIfAbsent(jobResult.getKey(), jobResult);

        if (curr != null) {
            throw new IllegalStateException(jobResult + " already exists in the job record results map!");
        }

        jobRepository.removeStartableJobAndResources(jobId);
    }

    public static class NonMemberCoordinatorPredicate
            implements Predicate<JobResultKey, JobResult>, IdentifiedDataSerializable {

        private Collection<Address> members;

        public NonMemberCoordinatorPredicate() {
        }

        public NonMemberCoordinatorPredicate(Collection<Address> members) {
            this.members = members;
        }

        @Override
        public boolean apply(Map.Entry<JobResultKey, JobResult> mapEntry) {
            return !members.contains(mapEntry.getKey().getCoordinator());
        }

        @Override
        public int getFactoryId() {
            return JetImplDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetImplDataSerializerHook.NON_MEMBER_COORDINATOR_PREDICATE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(members.size());
            for (Address member : members) {
                member.writeData(out);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int count = in.readInt();
            members = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Address member = new Address();
                member.readData(in);
                members.add(member);
            }
        }
    }

}
