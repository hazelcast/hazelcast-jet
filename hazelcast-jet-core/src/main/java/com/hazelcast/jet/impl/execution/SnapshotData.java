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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

/**
 * An object encapsulating snapshot information for a job in a {@link
 * JobRecord}.
 * <p>
 * It's immutable (except for readData), each "mutating" method returns a new
 * instance.
 */
public class SnapshotData implements IdentifiedDataSerializable {

    public static final int NO_SNAPSHOT = -1;

    /**
     * The ID of current successful snapshot. If {@link #dataMapIndex} < 0,
     * there's no successful snapshot.
     */
    private long snapshotId = NO_SNAPSHOT;

    /**
     * The data map index of current successful snapshot (0 or 1) or -1, if
     * there's no successful snapshot.
     */
    private int dataMapIndex = -1;

    /*
     * Stats for current successful snapshot.
     */
    private long startTime = Long.MIN_VALUE;
    private long endTime = Long.MIN_VALUE;
    private long numBytes;
    private long numKeys;
    private long numChunks;

    /**
     * ID for the ongoing snapshot. The value is incremented when we start a
     * new snapshot.
     * <p>
     * If {@code ongoingSnapshotId == }{@link #snapshotId}, there's no ongoing
     * snapshot. But if {@code ongoingSnapshotId > }{@link #snapshotId} it
     * doesn't mean a snapshot is ongoing: it will happen when a snapshot
     * fails. For next ongoing snapshot we'll increment the value again.
     */
    private long ongoingSnapshotId = NO_SNAPSHOT;
    private long ongoingSnapshotStartTime;

    /**
     * Contains the error message for the failure of the last snapshot.
     * if the last snapshot was successful, it's null.
     */
    @Nullable
    private String lastFailureText;

    public SnapshotData() {
    }

    // copy constructor
    private SnapshotData(SnapshotData copyFrom) {
        this.snapshotId = copyFrom.snapshotId;
        this.dataMapIndex = copyFrom.dataMapIndex;
        this.startTime = copyFrom.startTime;
        this.endTime = copyFrom.endTime;
        this.numBytes = copyFrom.numBytes;
        this.numKeys = copyFrom.numKeys;
        this.numChunks = copyFrom.numChunks;
        this.ongoingSnapshotId = copyFrom.ongoingSnapshotId;
        this.ongoingSnapshotStartTime = copyFrom.ongoingSnapshotStartTime;
        this.lastFailureText = copyFrom.lastFailureText;
    }

    @CheckReturnValue
    public SnapshotData startNewSnapshot() {
        SnapshotData res = new SnapshotData(this);
        res.ongoingSnapshotId++;
        res.ongoingSnapshotStartTime = Clock.currentTimeMillis();
        return res;
    }

    @CheckReturnValue
    public SnapshotData ongoingSnapshotDone(long numBytes, long numKeys, long numChunks, @Nullable String failureText) {
        SnapshotData res = new SnapshotData(this);
        res.lastFailureText = failureText;
        if (failureText == null) {
            res.snapshotId = ongoingSnapshotId;
            res.numBytes = numBytes;
            res.numKeys = numKeys;
            res.numChunks = numChunks;
            res.startTime = this.ongoingSnapshotStartTime;
            res.endTime = Clock.currentTimeMillis();
            res.dataMapIndex = ongoingDataMapIndex();
        } else {
            // we don't update the other fields because they only pertain to a successful snapshot
        }
        return res;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public int dataMapIndex() {
        return dataMapIndex;
    }

    /**
     * Returns the index of the data map into which the new snapshot will be
     * written.
     */
    public int ongoingDataMapIndex() {
        assert dataMapIndex == 0 // we'll return 1
                || dataMapIndex == 1 // we'll return 0
                || dataMapIndex == -1 // we'll return 0
                : "dataMapIndex=" + dataMapIndex;
        return (dataMapIndex + 1) & 1;
    }

    public long startTime() {
        return startTime;
    }

    public long endTime() {
        return endTime;
    }

    public long duration() {
        return endTime - startTime;
    }

    /**
     * Net number of bytes in primary copy. Doesn't include IMap overhead and backup copies.
     */
    public long numBytes() {
        return numBytes;
    }

    /**
     * Number of snapshot keys (after exploding chunks).
     */
    public long numKeys() {
        return numKeys;
    }

    /**
     * Number of chunks the snapshot is stored in. One chunk is one IMap entry,
     * so this is the number of entries in the data map.
     */
    public long numChunks() {
        return numChunks;
    }

    public long ongoingSnapshotId() {
        return ongoingSnapshotId;
    }

    public long ongoingSnapshotStartTime() {
        return ongoingSnapshotStartTime;
    }

    @Nullable
    public String lastFailureText() {
        return lastFailureText;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.SNAPSHOT_DATA;
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
        out.writeUTF(lastFailureText);
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
        lastFailureText = in.readUTF();
    }

    @Override
    public String toString() {
        return "SnapshotData{" +
                ", snapshotId=" + snapshotId +
                ", dataMapIndex=" + dataMapIndex +
                ", startTime=" + toLocalDateTime(startTime) +
                ", endTime=" + toLocalDateTime(endTime) +
                ", numBytes=" + numBytes +
                ", numKeys=" + numKeys +
                ", numChunks=" + numChunks +
                ", ongoingSnapshotId" + ongoingSnapshotId +
                ", ongoingSnapshotStartTime" + ongoingSnapshotStartTime +
                '}';
    }
}
