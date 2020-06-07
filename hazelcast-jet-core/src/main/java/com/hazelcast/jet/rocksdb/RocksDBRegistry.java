/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.rocksdb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry for state backend instances created for all running jobs.
 */
public final class RocksDBRegistry {
    private static final ConcurrentMap<Long, RocksDBStateBackend> INSTANCES = new ConcurrentHashMap<>();

    private RocksDBRegistry() {
    }

    /**
     * Lazily creates a new RocksDBStateBackend for a given job id.
     */
    public static RocksDBStateBackend getInstance(Long jobID) {
        return INSTANCES.computeIfAbsent(jobID, id -> new RocksDBStateBackend());
    }
}

