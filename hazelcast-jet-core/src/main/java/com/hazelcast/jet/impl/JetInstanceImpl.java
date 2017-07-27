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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStatus;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.JoinJobOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class JetInstanceImpl extends AbstractJetInstance {
    private final NodeEngine nodeEngine;
    private final JetConfig config;

    public JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance, JetConfig config) {
        super(hazelcastInstance);
        this.nodeEngine = hazelcastInstance.node.getNodeEngine();
        this.config = config;
    }

    @Override
    public JetConfig getConfig() {
        return config;
    }

    @Override
    public Job newJob(DAG dag) {
        JobImpl job = new JobImpl(dag, new JobConfig());
        job.init();
        return job;
    }

    @Override
    public Job newJob(DAG dag, JobConfig config) {
        JobImpl job = new JobImpl(dag, config);
        job.init();
        return job;
    }

    private class JobImpl extends AbstractJobImpl {

        JobImpl(DAG dag, JobConfig config) {
            super(JetInstanceImpl.this, dag, config);
        }

        @Override
        protected ICompletableFuture<Void> sendJoinRequest() {
            Operation op = new JoinJobOperation(getJobId());
            Address masterAddress = nodeEngine.getMasterAddress();
            return nodeEngine.getOperationService()
                                      .createInvocationBuilder(JetService.SERVICE_NAME, op, masterAddress)
                                      .invoke();
        }

        @Nonnull @Override
        public JobStatus getJobStatus() {
            try {
                Operation op = new GetJobStatusOperation(getJobId());
                Address masterAddress = nodeEngine.getMasterAddress();
                InternalCompletableFuture<JobStatus> f = nodeEngine.getOperationService()
                                                                   .createInvocationBuilder(JetService.SERVICE_NAME,
                                                                           op, masterAddress)
                                                                   .invoke();

                return f.get();
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }
}
