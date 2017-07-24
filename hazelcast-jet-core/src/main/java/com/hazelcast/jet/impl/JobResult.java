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

import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class JobResult implements IdentifiedDataSerializable {

    private JobResultKey key;

    private long creationTime;

    private long completionTime;

    private Throwable failure;

    public JobResult() {
    }

    public JobResult(long jobId, String coordinatorUUID, long creationTime, Long completionTime, Throwable failure) {
        this.key = new JobResultKey(jobId, coordinatorUUID);
        this.creationTime = creationTime;
        this.completionTime = completionTime;
        this.failure = failure;
    }

    public JobResultKey getKey() {
        return key;
    }

    public long getJobId() {
        return key.getJobId();
    }

    public String getCoordinatorUUID() {
        return key.getCoordinatorUUID();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public boolean isSuccessful() {
        return (failure == null);
    }

    public Throwable getFailure() {
        return failure;
    }

    public CompletableFuture<Throwable> asCompletableFuture() {
        CompletableFuture<Throwable> future = new CompletableFuture<>();
        future.complete(failure);

        return future;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobResult that = (JobResult) o;

        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "JobResult{" +
                "key=" + key +
                ", creationTime=" + creationTime +
                ", completionTime=" + completionTime +
                ", failure=" + failure +
                '}';
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.JOB_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        key.writeData(out);
        out.writeLong(creationTime);
        out.writeLong(completionTime);
        out.writeObject(failure);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = new JobResultKey();
        key.readData(in);
        creationTime = in.readLong();
        completionTime = in.readLong();
        failure = in.readObject();
    }

    public static class JobResultKey implements IdentifiedDataSerializable {

        private long jobId;
        private String coordinatorUUID;

        public JobResultKey() {
        }

        public JobResultKey(long jobId, String coordinatorUUID) {
            this.jobId = jobId;
            this.coordinatorUUID = coordinatorUUID;
        }

        public long getJobId() {
            return jobId;
        }

        public String getCoordinatorUUID() {
            return coordinatorUUID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JobResultKey that = (JobResultKey) o;

            return jobId == that.jobId && coordinatorUUID.equals(that.coordinatorUUID);
        }

        @Override
        public int hashCode() {
            int result = (int) (jobId ^ (jobId >>> 32));
            result = 31 * result + coordinatorUUID.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "JobResultKey{" +
                    "jobId=" + jobId +
                    ", coordinatorUUID=" + coordinatorUUID +
                    '}';
        }

        @Override
        public int getFactoryId() {
            return JetImplDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetImplDataSerializerHook.JOB_RESULT_KEY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(jobId);
            out.writeUTF(coordinatorUUID);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jobId = in.readLong();
            coordinatorUUID = in.readUTF();
        }
    }

}
