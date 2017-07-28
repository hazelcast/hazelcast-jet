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

import com.hazelcast.jet.SnapshotStorage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map.Entry;
import java.util.Queue;

import static com.hazelcast.jet.Util.entry;

class SnapshotStorageImpl implements SnapshotStorage {

    private final SerializationService serializationService;
    private final Queue<Object> snapshotQueue;
    private Entry<Data, Data> pendingEntry;

    SnapshotStorageImpl(SerializationService serializationService, Queue<Object> snapshotQueue) {
        this.serializationService = serializationService;
        this.snapshotQueue = snapshotQueue;
    }

    @Override
    public boolean offer(Object key, Object value) {
        if (pendingEntry != null) {
            if (!snapshotQueue.offer(pendingEntry)) {
                return false;
            }
        }

        // We serialize the key and value immediately to effectively clone them,
        // so the caller can modify them right after they are accepted by this method.
        // TODO use Map's partitioning strategy
        Data sKey = serializationService.toData(key);
        Data sValue = serializationService.toData(value);
        pendingEntry = entry(sKey, sValue);

        boolean success = snapshotQueue.offer(pendingEntry);
        if (success) {
            pendingEntry = null;
        }
        return success;
    }
}
