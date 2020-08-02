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

/**
 * Configuration Builder for RocksDBStateBackend options.
 */
public class RocksDBOptionsBuilder extends AbstractRocksDBOptionsBuilder {

    Integer cacheSize;

    /**
     * Sets the size of RocksDB block cache in bytes.
     */
    public RocksDBOptionsBuilder setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }
}
