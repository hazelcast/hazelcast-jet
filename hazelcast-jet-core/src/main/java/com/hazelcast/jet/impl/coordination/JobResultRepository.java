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
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.JobResult.JobResultKey;
import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class JobResultRepository {

    private static final String JOB_RESULTS_MAP_NAME = "__jet.jobs.results";

    private final Node node;
    private final ILogger logger;
    private final IMap<JobResultKey, JobResult> jobResults;

    public JobResultRepository(NodeEngineImpl nodeEngine) {
        this.node = nodeEngine.getNode();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobResults = nodeEngine.getHazelcastInstance().getMap(JOB_RESULTS_MAP_NAME);
    }

    public JobResult getResult(long jobId) {
        JobResultKey key = new JobResultKey(jobId, node.getThisUuid());
        JobResult jobResult = jobResults.get(key);

        if (jobResult != null) {
            return jobResult;
        }

        ClusterServiceImpl clusterService = node.getClusterService();
        List<String> memberAddresses = clusterService.getMembers().stream().map(Member::getUuid).collect(toList());
        Collection<JobResult> results = this.jobResults.values(new NonMemberCoordinatorPredicate(memberAddresses));

        if (results.isEmpty()) {
            return null;
        }

        if (results.size() > 1) {
            logger.info(results.size() + " job result records are found for job: "
                    + jobId + " -> " + results);
        }

        return results.iterator().next();
    }

    public void setResult(JobResult jobResult) {
        if (!(node.isMaster() && node.getThisAddress().equals(jobResult.getCoordinator()))) {
            throw new IllegalStateException("Cannot persist " + jobResult);
        }
        JobResult curr = jobResults.putIfAbsent(jobResult.getKey(), jobResult);
        if (curr != null) {
            throw new IllegalStateException(jobResult + " already exists in the job record results map!");
        }
    }

    public static class NonMemberCoordinatorPredicate
            implements Predicate<JobResultKey, JobResult>, IdentifiedDataSerializable {

        private Collection<String> members;

        // for deserialization only
        public NonMemberCoordinatorPredicate() {
        }

        public NonMemberCoordinatorPredicate(Collection<String> members) {
            this.members = members;
        }

        @Override
        public boolean apply(Map.Entry<JobResultKey, JobResult> mapEntry) {
            return !members.contains(mapEntry.getKey().getCoordinatorUUID());
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
            out.writeUTFArray((String[]) members.toArray());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            members = Arrays.asList(in.readUTFArray());
        }
    }

}
