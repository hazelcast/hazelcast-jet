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
import com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus;
import com.hazelcast.jet.impl.util.MaxByAggregator;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.query.Predicate;

import javax.annotation.Nullable;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.Util.compute;
import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotRepository {

    /**
     * Name of internal IMaps which store snapshot related data.
     * <p>
     * Snapshot metadata is stored in the following map:
     * <pre>SNAPSHOT_NAME_PREFIX + jobId</pre>
     * <p>
     * Snapshot data for one vertex is stored in the following map:
     * <pre>SNAPSHOT_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertexName</pre>
     */
    public static final String SNAPSHOT_NAME_PREFIX = "__jet.snapshots.";

    private static final long LATEST_STARTED_SNAPSHOT_ID_KEY = -1;

    private final JetInstance instance;

    SnapshotRepository(JetInstance jetInstance) {
        this.instance = jetInstance;
    }

    /**
     * Registers a new snapshot with a proposedId, and increment until the next free snapshot sequence
     * can be found. Returns the ID for the registered snapshot
     */
    long registerSnapshot(long jobId) {
        IStreamMap<Long, Object> snapshots = getSnapshotMap(jobId);

        SnapshotRecord record;
        do {
            long nextSnapshotId = generateNextSnapshotId(snapshots);
            record = new SnapshotRecord(jobId, nextSnapshotId);
        } while (snapshots.putIfAbsent(record.snapshotId(), record) != null);
        return record.snapshotId();
    }

    private long generateNextSnapshotId(IStreamMap<Long, Object> snapshots) {
        Long snapshotId;
        long nextSnapshotId;
        do {
            snapshotId = (Long) snapshots.get(LATEST_STARTED_SNAPSHOT_ID_KEY);
            nextSnapshotId = (snapshotId == null) ? 0 : (snapshotId + 1);
        } while (!replaceAllowingNull(snapshots, LATEST_STARTED_SNAPSHOT_ID_KEY, snapshotId, nextSnapshotId));

        return nextSnapshotId;
    }

    /**
     * Alternative for {@link IMap#replace(Object, Object, Object)} allowing null for {@code oldValue}.
     */
    private static <K, V> boolean replaceAllowingNull(IMap<K, V> map, K key, V oldValue, V newValue) {
        if (oldValue == null) {
            return map.putIfAbsent(key, newValue) == null;
        } else {
            return map.replace(key, oldValue, newValue);
        }
    }

    /**
     * Updates status of the given snapshot. Returns the elapsed time for the snapshot.
     */
    long setSnapshotStatus(long jobId, long snapshotId, SnapshotStatus status) {
        IStreamMap<Long, SnapshotRecord> snapshots = getSnapshotMap(jobId);
        SnapshotRecord record = compute(snapshots, snapshotId, (k, r) -> {
            r.setStatus(status);
            return r;
        });
        return System.currentTimeMillis() - record.startTime();
    }
    /**
     * Return the newest complete snapshot ID for the specified job or null if no such snapshot is found.
     */
    @Nullable
    Long latestCompleteSnapshot(long jobId) {
        IStreamMap<Long, Object> snapshotMap = getSnapshotMap(jobId);
        MaxByAggregator<Entry<Long, Object>> entryMaxByAggregator = maxByAggregator();
        Predicate<Long, Object> completedSnapshots = (Predicate<Long, Object>) e -> {
            Object value = e.getValue();
            return value instanceof SnapshotRecord && ((SnapshotRecord) value).isSuccessful();
        };
        Entry<Long, Object> entry = snapshotMap.aggregate(entryMaxByAggregator, completedSnapshots);
        return entry != null ? entry.getKey() : null;
    }

    /**
     * Return the latest started snapshot ID for the specified job or null if no such snapshot is found.
     */
    @Nullable
    Long latestStartedSnapshot(long jobId) {
        IMap<Long, Long> map = getSnapshotMap(jobId);
        return map.get(LATEST_STARTED_SNAPSHOT_ID_KEY);
    }

    private <T> IStreamMap<Long, T> getSnapshotMap(long jobId) {
        return instance.getMap(SNAPSHOT_NAME_PREFIX + idToString(jobId));
    }

    private MaxByAggregator<Entry<Long, Object>> maxByAggregator() {
        return new MaxByAggregator<>("snapshotId");
    }

    public static String snapshotDataMapName(long jobId, long snapshotId, String vertexName) {
        return SNAPSHOT_NAME_PREFIX + idToString(jobId) + '.' + snapshotId + '.' + vertexName;
    }
}
