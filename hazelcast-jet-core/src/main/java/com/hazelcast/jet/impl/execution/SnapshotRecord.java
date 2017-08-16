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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.idToString;

/**
 * A record stored in the {@link
 * com.hazelcast.jet.config.JetConfig#SNAPSHOT_RECORDS_MAP_NAME}
 * map.
 */
public class SnapshotRecord implements IdentifiedDataSerializable {

    private long jobId;
    private long snapshotId;
    private long creationTime = System.currentTimeMillis();
    private boolean isComplete;
    private List<String> vertices;

    public SnapshotRecord() {
    }

    public SnapshotRecord(long jobId, long snapshotId, List<String> vertices) {
        this.jobId = jobId;
        this.snapshotId = snapshotId;
        this.vertices = vertices;
    }

    /**
     * Return the jobId the snapshot was originally created for. Note that the
     * snapshot might be used to start another job.
     */
    public long jobId() {
        return jobId;
    }

    public long creationTime() {
        return creationTime;
    }

    public List<String> vertices() {
        return vertices;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void complete() {
        isComplete = true;
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.MASTER_SNAPSHOT_RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(creationTime);
        out.writeBoolean(isComplete);
        out.writeObject(vertices);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        creationTime = in.readLong();
        isComplete = in.readBoolean();
        vertices = in.readObject();
    }

    @Override
    public String toString() {
        return "SnapshotRecord{" +
                "jobId=" + idToString(jobId) +
                ", creationTime=" + Instant.ofEpochMilli(creationTime).atZone(ZoneId.systemDefault()).toLocalDateTime() +
                ", isComplete=" + isComplete +
                ", vertices=" + vertices +
                '}';
    }

    public long snapshotId() {
        return snapshotId;
    }
}
