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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.SnapshotValidationRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;

/**
 * A handle to exported state snapshot created using {@link
 * Job#exportSnapshot(String)}.
 */
public final class JobStateSnapshot {

    private final JetInstance instance;
    private final String name;
    private final SnapshotValidationRecord snapshotValidationRecord;

    JobStateSnapshot(@Nonnull JetInstance instance, @Nonnull String name) {
        this.instance = instance;
        this.name = name;

        IMapJet<Object, Object> map = instance.getMap(exportedSnapshotMapName(name));
        this.snapshotValidationRecord = (SnapshotValidationRecord) map.get(SnapshotValidationRecord.KEY);
        if (snapshotValidationRecord == null) {
            // By "touching" the map we've created it. There's no way to check for existence of IMap. If the
            // map is otherwise empty, let's destroy it.
            if (map.isEmpty()) {
                map.destroy();
            }
            throw new JetException("The underlying distributed object doesn't exist or it's not an exported state " +
                    "snapshot");
        }
    }

    /**
     * Returns the snapshot name. This is the name that was given to {@link
     * Job#exportSnapshot(String)}.
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * Returns the time the snapshot was created.
     */
    public long creationTime() {
        return snapshotValidationRecord.creationTime();
    }

    /**
     * Returns the job ID of the job the snapshot was originally exported from.
     */
    public long jobId() {
        return snapshotValidationRecord.jobId();
    }

    /**
     * Returns the job name of the job the snapshot was originally exported
     * from.
     */
    @Nullable
    public String jobName() {
        return snapshotValidationRecord.jobName();
    }

    /**
     * Returns the size in bytes of the payload data of the state snapshot.
     * Doesn't include storage overhead and especially doesn't account for
     * backup copies.
     */
    public long payloadSize() {
        return snapshotValidationRecord.numBytes();
    }

    /**
     * Returns the JSON representation of the DAG of the job this snapshot was
     * created from.
     */
    @Nonnull
    public String dagJsonString() {
        return snapshotValidationRecord.dagJsonString();
    }

    /**
     * Destroy the underlying distributed object.
     */
    public void destroy() {
        instance.getMap(exportedSnapshotMapName(name)).destroy();
    }
}
