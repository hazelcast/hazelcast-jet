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
import java.util.Set;

/**
 * A record stored in the {@link
 * com.hazelcast.jet.impl.coordination.MasterContext#SNAPSHOT_LIST_MAP_NAME}
 * map.
 */
public class MasterSnapshotRecord implements IdentifiedDataSerializable {
    private long creationTime = System.currentTimeMillis();
    private boolean isComplete;
    private Set<String> statefulVertexIds;

    public MasterSnapshotRecord() {
    }

    public MasterSnapshotRecord(Set<String> statefulVertexIds) {
        this.statefulVertexIds = statefulVertexIds;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Set<String> getStatefulVertexIds() {
        return statefulVertexIds;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void setComplete(boolean complete) {
        isComplete = complete;
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
        out.writeLong(creationTime);
        out.writeBoolean(isComplete);
        out.writeObject(statefulVertexIds);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        isComplete = in.readBoolean();
        statefulVertexIds = in.readObject();
    }

    @Override
    public String toString() {
        return "MasterSnapshotRecord{" +
                "creationTime=" + Instant.ofEpochMilli(creationTime).atZone(ZoneId.systemDefault()).toLocalDateTime() +
                ", isComplete=" + isComplete +
                ", statefulVertexIds=" + statefulVertexIds +
                '}';
    }
}
