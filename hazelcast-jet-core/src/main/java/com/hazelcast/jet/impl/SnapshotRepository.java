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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.execution.SnapshotData;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.jet.Jet.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

public class SnapshotRepository {

    /**
     * Prefix for internal IMaps which store snapshot data. Snapshot data for
     * one snapshot is stored in either of the following two maps:
     * <ul>
     *     <li>{@code _jet.snapshot.<jobId>.0}
     *     <li>{@code _jet.snapshot.<jobId>.1}
     * </ul>
     * Which one of these is determined in {@link SnapshotData}.
     */
    public static final String SNAPSHOT_DATA_MAP_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "snapshot.";

    private final JetInstance instance;
    private final ILogger logger;

    public SnapshotRepository(JetInstance jetInstance) {
        this.instance = jetInstance;
        this.logger = jetInstance.getHazelcastInstance().getLoggingService().getLogger(getClass());
    }

    /**
     * Returns map name in the form {@code "_jet.snapshot.<jobId>.<dataMapIndex>"}
     */
    public static String snapshotDataMapName(long jobId, int dataMapIndex) {
        return SNAPSHOT_DATA_MAP_PREFIX + idToString(jobId) + '.' + dataMapIndex;
    }

    /**
     * Delete all snapshots for a given job.
     */
    void destroySnapshotDataMaps(long jobId) {
        instance.getMap(snapshotDataMapName(jobId, 0)).destroy();
        instance.getMap(snapshotDataMapName(jobId, 1)).destroy();
        logFine(logger, "Destroyed both snapshot maps for job %s", idToString(jobId));
    }

    void clearSnapshotData(long jobId, int dataMapIndex) {
        String mapName = snapshotDataMapName(jobId, dataMapIndex);
        try {
            instance.getMap(mapName).clear();
            logFine(logger, "Cleared snapshot data map %s", mapName);
        } catch (Exception logged) {
            logger.warning("Cannot delete old snapshot data  " + idToString(jobId), logged);
        }
    }
}
