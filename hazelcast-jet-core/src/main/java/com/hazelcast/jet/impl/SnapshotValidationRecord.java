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

import javax.annotation.Nonnull;
import java.io.Serializable;

public class SnapshotValidationRecord implements Serializable {
    public static final SnapshotValidationKey KEY = SnapshotValidationKey.KEY;

    private final long snapshotId;
    private final long numChunks;
    private final long numBytes;

    private final long creationTime;
    private final long jobId;
    private final String jobName;
    private final String dagJsonString;

    public SnapshotValidationRecord(long snapshotId, long numChunks, long numBytes, long creationTime, long jobId,
                                    @Nonnull String jobName, @Nonnull String dagJsonString) {
        this.snapshotId = snapshotId;
        this.numChunks = numChunks;
        this.numBytes = numBytes;
        this.creationTime = creationTime;
        this.jobId = jobId;
        this.jobName = jobName;
        this.dagJsonString = dagJsonString;
    }

    public long snapshotId() {
        return snapshotId;
    }

    long numChunks() {
        return numChunks;
    }

    public long numBytes() {
        return numBytes;
    }

    public long creationTime() {
        return creationTime;
    }

    public long jobId() {
        return jobId;
    }

    public String jobName() {
        return jobName;
    }

    public String dagJsonString() {
        return dagJsonString;
    }

    enum SnapshotValidationKey {
        KEY
    }
}
