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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.util.MaxByAggregator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotRepository {

    /**
     * Name of internal IMap which stores snapshot ids
     */
    public static final String SNAPSHOT_RECORDS_MAP_NAME = "__jet.snapshots";

    /**
     * Name of internal IMap which stores snapshot data. This a prefix, the
     * format is:
     * <pre>SNAPSHOT_DATA_MAP_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertexName</pre>
     */
    public static final String SNAPSHOT_DATA_MAP_NAME_PREFIX = "__jet.snapshots.";

    private final IMap<List<Long>, SnapshotRecord> snapshotsMap;
    private final ILogger logger;

    SnapshotRepository(JetInstance jetInstance) {
        this.snapshotsMap = jetInstance.getHazelcastInstance().getMap(SNAPSHOT_RECORDS_MAP_NAME);
        this.logger = jetInstance.getHazelcastInstance().getLoggingService().getLogger(getClass());
    }

    /**
     * Create new {@link SnapshotRecord} for the supplied jobId, trying to use
     * snapshotId of {@code proposedId}, then {@code proposedId + 1} etc.
     */
    SnapshotRecord putNewRecord(long jobId, long proposedId) {
        SnapshotRecord record;
        do {
            record = new SnapshotRecord(jobId, proposedId++);
        } while (snapshotsMap.putIfAbsent(Arrays.asList(record.jobId(), record.snapshotId()), record) != null);
        return record;
    }

    /**
     * Return snapshotId of newest complete snapshot for the specified job.
     */
    SnapshotRecord findLatestSnapshot(long jobId) {
        EntryObject eo = new PredicateBuilder().getEntryObject();
        Entry<List, SnapshotRecord> newestSnapshot =
                snapshotsMap.aggregate(new MaxByAggregator<>("snapshotId"),
                        eo.get("jobId").equal(jobId)
                        .and(eo.get("complete").equal(true)));
        return newestSnapshot != null ? newestSnapshot.getValue() : null;
    }

    void markRecordCompleted(long jobId, long snapshotId) {
        long creationTime = (long) snapshotsMap.executeOnKey(Arrays.asList(jobId, snapshotId),
                new MarkRecordCompleteEntryProcessor());

        logger.info(String.format("Snapshot %s for job %s completed in %dms", snapshotId,
                idToString(jobId), System.currentTimeMillis() - creationTime));
    }

    public static String snapshotDataMapName(long jobId, long snapshotId, String vertexName) {
        return SNAPSHOT_DATA_MAP_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertexName;
    }

    private static class MarkRecordCompleteEntryProcessor extends AbstractEntryProcessor<Object, SnapshotRecord> {
        @Override
        public Object process(Entry<Object, SnapshotRecord> entry) {
            SnapshotRecord record = entry.getValue();
            record.setComplete();
            entry.setValue(record);
            return record.startTime();
        }
    }
}
