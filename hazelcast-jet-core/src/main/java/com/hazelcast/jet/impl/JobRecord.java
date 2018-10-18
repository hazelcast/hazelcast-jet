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
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
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
 */
public class JobRecord implements IdentifiedDataSerializable {

    private long jobId;
    private long creationTime;
    private Data dag;
    // JSON representation of DAG, used by management center
    private String dagJson;
    private JobConfig config;

    private DynamicData dynamicData = new DynamicData();

    public JobRecord() {
    }

    public JobRecord(long jobId, long creationTime, Data dag, String dagJson, JobConfig config, int quorumSize,
                     boolean suspended) {
        this.jobId = jobId;
        this.creationTime = creationTime;
        this.dag = dag;
        this.dagJson = dagJson;
        this.config = config;
        dynamicData.quorumSize.set(quorumSize);
        dynamicData.suspended = suspended;
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

    public String successfulSnapshotDataMapName() {
        if (snapshotId() < 0) {
            throw new IllegalStateException("No successful snapshot");
        }
        return snapshotDataMapName(jobId, dataMapIndex());
    }

    public long getTimestamp() {
        return dynamicData.timestamp.get();
    }

    public int getQuorumSize() {
        return dynamicData.quorumSize.get();
    }

    /**
     * Updates the quorum size if it's larger than the current value. Ignores, if it's not.
     */
    void setLargerQuorumSize(int newQuorumSize) {
        dynamicData.quorumSize.getAndAccumulate(newQuorumSize, Math::max);
    }

    public boolean isSuspended() {
        return dynamicData.suspended;
    }

    public void setSuspended(boolean suspended) {
        this.dynamicData.suspended = suspended;
    }

    void startNewSnapshot() {
        dynamicData.ongoingSnapshotId++;
        dynamicData.ongoingSnapshotStartTime = Clock.currentTimeMillis();
    }

    void ongoingSnapshotDone(long numBytes, long numKeys, long numChunks, @Nullable String failureText) {
        dynamicData.lastFailureText = failureText;
        if (failureText == null) {
            dynamicData.snapshotId = dynamicData.ongoingSnapshotId;
            dynamicData.numBytes = numBytes;
            dynamicData.numKeys = numKeys;
            dynamicData.numChunks = numChunks;
            dynamicData.startTime = this.dynamicData.ongoingSnapshotStartTime;
            dynamicData.endTime = Clock.currentTimeMillis();
            dynamicData.dataMapIndex = ongoingDataMapIndex();
        } else {
            // we don't update the other fields because they only pertain to a successful snapshot
        }
    }

    public long snapshotId() {
        return dynamicData.snapshotId;
    }

    public int dataMapIndex() {
        return dynamicData.dataMapIndex;
    }

    /**
     * Returns the index of the data map into which the new snapshot will be
     * written.
     */
    int ongoingDataMapIndex() {
        assert dynamicData.dataMapIndex == 0 // we'll return 1
                || dynamicData.dataMapIndex == 1 // we'll return 0
                || dynamicData.dataMapIndex == -1 // we'll return 0
                : "dataMapIndex=" + dynamicData.dataMapIndex;
        return (dynamicData.dataMapIndex + 1) & 1;
    }

    public long startTime() {
        return dynamicData.startTime;
    }

    public long endTime() {
        return dynamicData.endTime;
    }

    public long duration() {
        return dynamicData.endTime - dynamicData.startTime;
    }

    /**
     * Net number of bytes in primary copy. Doesn't include IMap overhead and backup copies.
     */
    public long numBytes() {
        return dynamicData.numBytes;
    }

    /**
     * Number of snapshot keys (after exploding chunks).
     */
    public long numKeys() {
        return dynamicData.numKeys;
    }

    /**
     * Number of chunks the snapshot is stored in. One chunk is one IMap entry,
     * so this is the number of entries in the data map.
     */
    public long numChunks() {
        return dynamicData.numChunks;
    }

    public long ongoingSnapshotId() {
        return dynamicData.ongoingSnapshotId;
    }

    public long ongoingSnapshotStartTime() {
        return dynamicData.ongoingSnapshotStartTime;
    }

    @Nullable
    public String lastFailureText() {
        return dynamicData.lastFailureText;
    }

    DynamicData getDynamicData() {
        return dynamicData;
    }

    void setDynamicData(DynamicData dynamicData) {
        this.dynamicData = dynamicData;
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
        out.writeObject(dynamicData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        creationTime = in.readLong();
        dag = in.readData();
        dagJson = in.readUTF();
        config = in.readObject();
        dynamicData = in.readObject();
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobId=" + idToString(jobId) +
                ", name=" + getConfig().getName() +
                ", creationTime=" + toLocalDateTime(creationTime) +
                ", dagJson=" + dagJson +
                ", config=" + config +
                ", dynamicData=" + dynamicData +
                '}';
    }

    /**
     * An object encapsulating mutable part of information for a job in a {@link
     * JobRecord}.
     * <p>
     * The reason for this split is that the information are updated quite
     * frequently and the {@link JobRecord} contains other slow-to-serialize fields
     * which would harm performance (JobConfig, DAG...).
     */
    public static class DynamicData implements IdentifiedDataSerializable {

        public static final int NO_SNAPSHOT = -1;

        /**
         * Timestamp to order async updates to the JobRecord. See {@link
         * JobRepository#writeJobRecordSync} and {@link
         * JobRepository#writeJobRecordAsync}.
         */
        private final AtomicLong timestamp = new AtomicLong();

        private final AtomicInteger quorumSize = new AtomicInteger();
        private volatile boolean suspended;

        /**
         * The ID of current successful snapshot. If {@link #dataMapIndex} < 0,
         * there's no successful snapshot.
         */
        private volatile long snapshotId = NO_SNAPSHOT;

        /**
         * The data map index of current successful snapshot (0 or 1) or -1, if
         * there's no successful snapshot.
         */
        private volatile int dataMapIndex = -1;

        /*
         * Stats for current successful snapshot.
         */
        private volatile long startTime = Long.MIN_VALUE;
        private volatile long endTime = Long.MIN_VALUE;
        private volatile long numBytes;
        private volatile long numKeys;
        private volatile long numChunks;

        /**
         * ID for the ongoing snapshot. The value is incremented when we start a
         * new snapshot.
         * <p>
         * If {@code ongoingSnapshotId == }{@link #snapshotId}, there's no ongoing
         * snapshot. But if {@code ongoingSnapshotId > }{@link #snapshotId} it
         * doesn't mean a snapshot is ongoing: it will happen when a snapshot
         * fails. For next ongoing snapshot we'll increment the value again.
         */
        private volatile long ongoingSnapshotId = NO_SNAPSHOT;
        private volatile long ongoingSnapshotStartTime;

        /**
         * Contains the error message for the failure of the last snapshot.
         * if the last snapshot was successful, it's null.
         */
        @Nullable
        private volatile String lastFailureText;

        public DynamicData() {
        }

        long getTimestamp() {
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
            return JetInitDataSerializerHook.DYNAMIC_DATA;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(snapshotId);
            out.writeInt(dataMapIndex);
            out.writeLong(startTime);
            out.writeLong(endTime);
            out.writeLong(numBytes);
            out.writeLong(numKeys);
            out.writeLong(numChunks);
            out.writeLong(ongoingSnapshotId);
            out.writeLong(ongoingSnapshotStartTime);
            out.writeBoolean(lastFailureText != null);
            if (lastFailureText != null) {
                out.writeUTF(lastFailureText);
            }
            out.writeInt(quorumSize.get());
            out.writeBoolean(suspended);
            out.writeLong(timestamp.get());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            snapshotId = in.readLong();
            dataMapIndex = in.readInt();
            startTime = in.readLong();
            endTime = in.readLong();
            numBytes = in.readLong();
            numKeys = in.readLong();
            numChunks = in.readLong();
            ongoingSnapshotId = in.readLong();
            ongoingSnapshotStartTime = in.readLong();
            lastFailureText = in.readBoolean() ? in.readUTF() : null;
            quorumSize.set(in.readInt());
            suspended = in.readBoolean();
            timestamp.set(in.readLong());
        }

        @Override
        public String toString() {
            return "DynamicData{" +
                    "timestamp=" + timestamp.get() +
                    ", quorumSize=" + quorumSize +
                    ", suspended=" + suspended +
                    ", snapshotId=" + snapshotId +
                    ", dataMapIndex=" + dataMapIndex +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", numBytes=" + numBytes +
                    ", numKeys=" + numKeys +
                    ", numChunks=" + numChunks +
                    ", ongoingSnapshotId=" + ongoingSnapshotId +
                    ", ongoingSnapshotStartTime=" + ongoingSnapshotStartTime +
                    ", lastFailureText='" + lastFailureText + '\'' +
                    '}';
        }
    }
}
