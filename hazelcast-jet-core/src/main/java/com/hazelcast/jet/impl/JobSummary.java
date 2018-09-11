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

import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;

public class JobSummary implements IdentifiedDataSerializable {

    private long jobId;

    private long executionId;
    private String name;
    private JobStatus status;
    private long submissionTime;

    public JobSummary() {
    }

    public JobSummary(long jobId, long executionId, String name, JobStatus status, long submissionTime) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.name = name;
        this.status = status;
        this.submissionTime = submissionTime;
    }

    public long getJobId() {
        return jobId;
    }

    /**
     * Returns execution id of the job if running, or 0 otherwise
     */
    public long getExecutionId() {
        return executionId;
    }

    public String getName() {
        return name;
    }

    public JobStatus getStatus() {
        return status;
    }

    public long getSubmissionTime() {
        return submissionTime;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.JOB_SUMMARY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(executionId);
        out.writeUTF(name);
        out.writeObject(status);
        out.writeLong(submissionTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        executionId = in.readLong();
        name = in.readUTF();
        status = in.readObject();
        submissionTime = in.readLong();
    }

    @Override
    public String toString() {
        return "JobSummary{" +
                "jobId=" + idToString(jobId) +
                ", executionId=" + idToString(executionId) +
                ", name='" + name + '\'' +
                ", status=" + status +
                ", submissionTime=" + toLocalTime(submissionTime) +
                '}';
    }
}
