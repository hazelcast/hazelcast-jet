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
import java.util.Set;

import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

/**
 * A record stored in the {@link
 * com.hazelcast.jet.impl.SnapshotRepository#SNAPSHOT_RECORDS_MAP_NAME}
 * map.
 */
public class SnapshotRecord implements IdentifiedDataSerializable {

    private long jobId;
    private long snapshotId;
    private long startTime = System.currentTimeMillis();
    private boolean complete;
    private Set<String> vertices;

    public SnapshotRecord() {
    }

    public SnapshotRecord(long jobId, long snapshotId, Set<String> vertices) {
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

    public Set<String> vertices() {
        return vertices;
    }

    public boolean complete() {
        return complete;
    }

    public void setComplete() {
        complete = true;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long startTime() {
        return startTime;
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
        out.writeLong(snapshotId);
        out.writeLong(startTime);
        out.writeBoolean(complete);
        out.writeObject(vertices);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        snapshotId = in.readLong();
        startTime = in.readLong();
        complete = in.readBoolean();
        vertices = in.readObject();
    }

    @Override public String toString() {
        return "SnapshotRecord{" +
                "jobId=" + idToString(jobId) +
                ", snapshotId=" + snapshotId +
                ", startTime=" + toLocalDateTime(startTime) +
                ", complete=" + complete +
                ", vertices=" + vertices +
                '}';
    }
}
