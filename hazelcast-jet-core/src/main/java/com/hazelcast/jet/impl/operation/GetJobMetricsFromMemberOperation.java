/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.core.JobMetrics;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;

/**
 * An operation sent from the master to all members to query metrics for a
 * specific job ID.
 */
public class GetJobMetricsFromMemberOperation extends AbstractJobOperation {

    private long executionId;
    private JobMetrics response;

    public GetJobMetricsFromMemberOperation() {
    }

    public GetJobMetricsFromMemberOperation(long jobId, long executionId) {
        super(jobId);
        this.executionId = executionId;
    }

    @Override
    public void run() {
        JetService service = getService();
        ExecutionContext executionContext = service.getJobExecutionService().getExecutionContext(executionId);
        if (executionContext == null) {
            response = JobMetrics.EMPTY;
        } else {
            response = executionContext.getJobMetrics();
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.GET_JOB_METRICS_FROM_MEMBER_OP;
    }
}
