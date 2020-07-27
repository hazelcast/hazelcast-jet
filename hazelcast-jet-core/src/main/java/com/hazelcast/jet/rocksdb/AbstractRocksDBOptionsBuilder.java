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

import java.io.Serializable;

/**
 * Base class of configuration builder for RocksDB options.
 * An instance of this class is passed to JobConfig to set custom options for RocksDB.
 */
public abstract class AbstractRocksDBOptionsBuilder implements Serializable {
    Integer memtableSize;
    Integer memtableNumber;
    Integer bloomFilterBits;

    /**
     * Sets RocksDB MemTable Size in bytes.
     */
    public AbstractRocksDBOptionsBuilder setMemtableSize(int memtableSize) {
        this.memtableSize = memtableSize;
        return this;
    }

    /**
     * Sets the number of RocksDB MemTables per ColumnFamily.
     */
    public AbstractRocksDBOptionsBuilder setMemtableNumber(int memtableNumber) {
        this.memtableNumber = memtableNumber;
        return this;
    }

    /**
     * Sets the number of bits used for RocksDB bloom filter.
     */
    public AbstractRocksDBOptionsBuilder setBloomFilterBits(int bloomFilterBits) {
        this.bloomFilterBits = bloomFilterBits;
        return this;
    }
}
