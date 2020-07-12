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

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.WriteOptions;

import java.io.Serializable;

/**
 * General RocksDB Configurations
 */
public class RocksDBOptions implements Serializable {

    private int memtableSize = 64 * 1024 * 1024;
    private int memtableNumber = 4;
    private int bloomBits = 10;
    private int cacheSize = 265 * 1024 * 1024;

    RocksDBOptions setOptions(RocksDBOptions options) {
        this.memtableNumber = options.memtableNumber;
        this.memtableSize = options.memtableSize;
        this.bloomBits = options.bloomBits;
        this.cacheSize = options.cacheSize;
        return this;
    }

    /**
     * Sets RocksDB MemTable Size in bytes.
     */
    public RocksDBOptions setMemtableSize(int memtableSize) {
        this.memtableSize = memtableSize;
        return this;
    }

    /**
     * Sets the number of RocksDB MemTables per ColumnFamily.
     */
    public RocksDBOptions setMemtableNumber(int memtableNumber) {
        this.memtableNumber = memtableNumber;
        return this;
    }

    /**
     * Sets the number of bits used for RocksDB bloom filter.
     */
    public RocksDBOptions setBloomBits(int bloomBits) {
        this.bloomBits = bloomBits;
        return this;
    }

    /**
     * Sets the size of RocksDB block cache in bytes.
     */
    public RocksDBOptions setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    Options options() {
        return new Options().setCreateIfMissing(true);
    }

    ColumnFamilyOptions columnFamilyOptions() {
        return new ColumnFamilyOptions()
                .setMaxWriteBufferNumber(memtableNumber)
                .setWriteBufferSize(memtableSize)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockCacheSize(cacheSize)
                        .setPinL0FilterAndIndexBlocksInCache(true)
                        .setFilter(new BloomFilter(bloomBits)));
    }

    WriteOptions writeOptions() {
        return new WriteOptions().setDisableWAL(true).setNoSlowdown(true);
    }

    ReadOptions readOptions() {
        return new ReadOptions();
    }
}
