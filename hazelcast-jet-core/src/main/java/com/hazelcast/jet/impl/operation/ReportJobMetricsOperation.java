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

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;

import java.util.Map;

public class ReportJobMetricsOperation extends AbstractJobOperation {

    private long executionId;
    private Map<String, Long> response;

    public ReportJobMetricsOperation() {
    }

    public ReportJobMetricsOperation(long jobId, long executionId) {
        super(jobId);
        this.executionId = executionId;
    }

    @Override
    public void run() {
        JetService service = getService();
        response = JobMetricsUtil.getJobMetrics(service, executionId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.REPORT_METRICS_OP;
    }

}
