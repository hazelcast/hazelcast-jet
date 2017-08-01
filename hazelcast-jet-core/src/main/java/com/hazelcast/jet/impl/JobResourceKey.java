package com.hazelcast.jet.impl;

import com.hazelcast.core.PartitionAware;
import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class JobResourceKey implements IdentifiedDataSerializable, PartitionAware<Long> {

    private long jobId;

    private String resourceName;

    public JobResourceKey() {
    }

    public JobResourceKey(long jobId, String resourceName) {
        this.jobId = jobId;
        this.resourceName = resourceName;
    }

    public long getJobId() {
        return jobId;
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public Long getPartitionKey() {
        return jobId;
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.JOB_RESOURCE_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeUTF(resourceName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        resourceName = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobResourceKey that = (JobResourceKey) o;

        if (jobId != that.jobId) {
            return false;
        }
        return resourceName.equals(that.resourceName);
    }

    @Override
    public int hashCode() {
        int result = (int) (jobId ^ (jobId >>> 32));
        result = 31 * result + resourceName.hashCode();
        return result;
    }
}
