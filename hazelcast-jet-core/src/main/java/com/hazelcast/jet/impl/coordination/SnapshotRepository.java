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

package com.hazelcast.jet.impl.coordination;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.util.MaxByAggregator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;

import java.util.Arrays;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotRepository {

    private final IMap<Object, Object> snapshotsMap;
    private final ILogger logger;

    public SnapshotRepository(JetInstance jetInstance) {
        this.snapshotsMap = jetInstance.getHazelcastInstance().getMap(JetConfig.SNAPSHOT_RECORDS_MAP_NAME);
        this.logger = jetInstance.getHazelcastInstance().getLoggingService().getLogger(getClass());
    }

    boolean putNewSnapshotRecord(SnapshotRecord record) {
        if (snapshotsMap.putIfAbsent(Arrays.asList(record.jobId(), record.snapshotId()), record) != null) {
            logger.severe("Snapshot with id " + record.snapshotId() + " already exists for job "
                    + idToString(record.jobId()));
            //TODO: should job be failed here?
            return false;
        }
        return true;
    }

    /**
     * Return snapshotId of newest complete snapshot for the specified job.
     */
    SnapshotRecord findUsableSnapshot(long jobId) {
        EntryObject eo = new PredicateBuilder().getEntryObject();
        Entry<Long, SnapshotRecord> newestSnapshot =
                snapshotsMap.aggregate(new MaxByAggregator<>("creationTime"),
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
        return JetConfig.SNAPSHOT_DATA_MAP_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertexName;
    }

    private static class MarkRecordCompleteEntryProcessor extends AbstractEntryProcessor<Object, SnapshotRecord> {
        @Override
        public Object process(Entry<Object, SnapshotRecord> entry) {
            SnapshotRecord record = entry.getValue();
            record.complete();
            entry.setValue(record);
            return record.creationTime();
        }
    }
}
