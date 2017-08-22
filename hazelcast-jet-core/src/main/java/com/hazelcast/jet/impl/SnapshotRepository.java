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

package com.hazelcast.jet.impl;

import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.util.MaxByAggregator;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;

import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.compute;
import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotRepository {

    /**
     * Name of internal IMaps which stores snapshot related data.
     *
     * Snapshot metadata is stored in the following map:
     * <pre>SNAPSHOT_NAME_PREFIX + jobId/pre>
     *
     * Snapshot data is stored in the following map:
     * <pre>SNAPSHOT_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertexName</pre>
     */
    public static final String SNAPSHOT_NAME_PREFIX = "__jet.snapshots.";

    private final ILogger logger;
    private final JetInstance instance;

    public SnapshotRepository(JetInstance jetInstance) {
        this.instance = jetInstance;
        this.logger = jetInstance.getHazelcastInstance().getLoggingService().getLogger(getClass());
    }

    long beginSnapshot(long jobId, long proposedId) {
        IStreamMap<Long, SnapshotRecord> snapshots = getSnapshotMap(jobId);
        SnapshotRecord record;
        do {
            record = new SnapshotRecord(jobId, proposedId++);
        } while (snapshots.putIfAbsent(record.snapshotId(), record) != null);
        return record.snapshotId();
    }

    private IStreamMap<Long, SnapshotRecord> getSnapshotMap(long jobId) {
        return instance.getMap(SNAPSHOT_NAME_PREFIX + idToString(jobId));
    }

    /**
     * Return the newest complete snapshot ID for the specified job.
     */
    Long latestCompleteSnapshot(long jobId) {
        Predicate<Long, SnapshotRecord> completedSnapshots = mapEntry -> mapEntry.getValue().complete();
        Entry<Long, SnapshotRecord> entry = getSnapshotMap(jobId).aggregate(maxByAggregator(), completedSnapshots);
        return entry != null ? entry.getKey() : null;
    }

    /**
     * Return the latest started snapshot ID for the specified job.
     */
    Long latestStartedSnapshot(long jobId) {
        Entry<Long, SnapshotRecord> entry = getSnapshotMap(jobId).aggregate(maxByAggregator());
        return entry != null ? entry.getKey() : null;
    }

    private MaxByAggregator<Entry<Long, SnapshotRecord>> maxByAggregator() {
        return new MaxByAggregator<>("snapshotId");
    }

    void markRecordCompleted(long jobId, long snapshotId) {
        IStreamMap<Long, SnapshotRecord> snapshots = getSnapshotMap(jobId);
        SnapshotRecord record = compute(snapshots, snapshotId, (k, r) -> {
            r.setComplete();
            return r;
        });
        logger.info(String.format("Snapshot %s for job %s completed in %dms", snapshotId,
                idToString(jobId), System.currentTimeMillis() - record.startTime()));
    }

    public static String snapshotDataMapName(long jobId, long snapshotId, String vertexName) {
        return SNAPSHOT_NAME_PREFIX + idToString(jobId) + '.' + snapshotId + '.' + vertexName;
    }
}
