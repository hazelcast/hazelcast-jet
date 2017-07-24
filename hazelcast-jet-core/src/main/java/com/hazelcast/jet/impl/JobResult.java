package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.nio.Address;
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

    public JobResult(long jobId, Address coordinator, long creationTime, Long completionTime, Throwable failure) {
        this.key = new JobResultKey(jobId, coordinator);
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

    public Address getCoordinator() {
        return key.getCoordinator();
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

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

        private Address coordinator;

        public JobResultKey() {
        }

        public JobResultKey(long jobId, Address coordinator) {
            this.jobId = jobId;
            this.coordinator = coordinator;
        }

        public long getJobId() {
            return jobId;
        }

        public Address getCoordinator() {
            return coordinator;
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

            return jobId == that.jobId && coordinator.equals(that.coordinator);
        }

        @Override
        public int hashCode() {
            int result = (int) (jobId ^ (jobId >>> 32));
            result = 31 * result + coordinator.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "JobResultKey{" +
                    "jobId=" + jobId +
                    ", coordinator=" + coordinator +
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
            coordinator.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jobId = in.readLong();
            coordinator = new Address();
            coordinator.readData(in);
        }
    }

}
