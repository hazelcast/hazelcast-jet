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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.SnapshotData;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

/**
 * Metadata information about the job. There's one instance for each jobId,
 * used across multiple executions.
 * <p>
 * It should be updated only through MasterContext class, where multiple
 * updates are synchronized.
 *
 * TODO [viliam] split static and dynamic data for less serialization
 */
public class JobRecord implements IdentifiedDataSerializable {

    private long jobId;
    private long creationTime;
    private Data dag;
    // JSON representation of DAG, used by management center
    private String dagJson;
    private JobConfig config;
    private volatile int quorumSize;
    private volatile boolean suspended;

    private volatile SnapshotData snapshotData = new SnapshotData();

    /**
     * Timestamp to order async updates to the JobRecord. See {@link
     * JobRepository#writeJobRecordSync} and {@link
     * JobRepository#writeJobRecordAsync}.
     */
    private final AtomicLong timestamp = new AtomicLong();

    public JobRecord() {
    }

    public JobRecord(long jobId, long creationTime, Data dag, String dagJson, JobConfig config, int quorumSize,
                     boolean suspended) {
        this.jobId = jobId;
        this.creationTime = creationTime;
        this.dag = dag;
        this.dagJson = dagJson;
        this.config = config;
        this.quorumSize = quorumSize;
        this.suspended = suspended;
    }

    public long getJobId() {
        return jobId;
    }

    public String getJobNameOrId() {
        return config.getName() != null ? config.getName() : idToString(jobId);
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Data getDag() {
        return dag;
    }

    // used by ManCenter
    public String getDagJson() {
        return dagJson;
    }

    public JobConfig getConfig() {
        return config;
    }

    public int getQuorumSize() {
        return quorumSize;
    }

    public void setQuorumSize(int newQuorumSize) {
        if (newQuorumSize <= quorumSize) {
            throw new IllegalArgumentException("New quorum size: " + newQuorumSize
                    + " must be bigger than current quorum size of " + this);
        }
        this.quorumSize = newQuorumSize;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    @Nonnull
    public SnapshotData getSnapshotData() {
        return snapshotData;
    }

    public void setSnapshotData(SnapshotData snapshotData) {
        this.snapshotData = snapshotData;
    }

    public String successfulSnapshotDataMapName() {
        if (snapshotData.snapshotId() < 0) {
            throw new IllegalStateException("No successful snapshot");
        }
        return snapshotDataMapName(jobId, snapshotData.dataMapIndex());
    }

    public long getTimestamp() {
        return timestamp.get();
    }

    /**
     * Sets the timestamp to:
     *   <pre>max(Clock.currentTimeMillis(), this.timestamp + 1);</pre>
     * <p>
     * In other words, after this call the timestamp is guaranteed to be
     * incremented by at least 1 and be no smaller than the current wall clock
     * time.
     */
    void updateTimestamp() {
        timestamp.updateAndGet(v -> Math.max(Clock.currentTimeMillis(), v + 1));
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.JOB_RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(creationTime);
        out.writeData(dag);
        out.writeUTF(dagJson);
        out.writeObject(config);
        out.writeInt(quorumSize);
        out.writeBoolean(suspended);
        snapshotData.writeData(out);
        out.writeLong(timestamp.get());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        creationTime = in.readLong();
        dag = in.readData();
        dagJson = in.readUTF();
        config = in.readObject();
        quorumSize = in.readInt();
        suspended = in.readBoolean();
        snapshotData.readData(in);
        timestamp.set(in.readLong());
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobId=" + idToString(jobId) +
                ", name=" + getConfig().getName() +
                ", creationTime=" + toLocalDateTime(creationTime) +
                ", dagJson=" + dagJson +
                ", config=" + config +
                ", quorumSize=" + quorumSize +
                ", suspended=" + suspended +
                ", snapshotData=" + snapshotData +
                ", timestamp=" + timestamp +
                '}';
    }
}
