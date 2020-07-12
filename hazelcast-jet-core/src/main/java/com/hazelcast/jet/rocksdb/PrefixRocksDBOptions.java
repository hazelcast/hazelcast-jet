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
import org.rocksdb.FlushOptions;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.WriteOptions;

import java.io.Serializable;

/**
 * RocksDB configurations suitable for bulk-load then prefix scan use case.
 */
public class PrefixRocksDBOptions implements Serializable {

    private static final int MEMTABLE_SIZE = 128 * 1024 * 1024;
    private static final int MEMTABLE_NUMBER = 4;
    private static final int BLOOM_BITS = 10;
    private int memtableSize;
    private int memtableNumber;
    private int bloomBits;

    /**
     * Creates a new PrefixRocksDBOptions instance with default options.
     */
    public PrefixRocksDBOptions() {
        this.memtableSize = MEMTABLE_SIZE;
        this.memtableNumber = MEMTABLE_NUMBER;
        this.bloomBits = BLOOM_BITS;
    }

    PrefixRocksDBOptions(PrefixRocksDBOptions options) {
        this.memtableSize = options.memtableSize;
        this.memtableNumber = options.memtableNumber;
        this.bloomBits = options.bloomBits;
    }

    /**
     * Sets RocksDB MemTable Size in bytes.
     */
    public PrefixRocksDBOptions setMemtableSize(int memtableSize) {
        this.memtableSize = memtableSize;
        return this;
    }

    /**
     * Sets the number of RocksDB MemTables per ColumnFamily.
     */
    public PrefixRocksDBOptions setMemtableNumber(int memtableNumber) {
        this.memtableNumber = memtableNumber;
        return this;
    }

    /**
     * Sets the number of bits used for RocksDB bloom filter.
     */
    public PrefixRocksDBOptions setBloomBits(int bloomBits) {
        this.bloomBits = bloomBits;
        return this;
    }


    Options options() {
        return new Options()
                .setCreateIfMissing(true)
                .setInfoLogLevel(InfoLogLevel.ERROR_LEVEL)
                .prepareForBulkLoad()
                .setAllowConcurrentMemtableWrite(false)
                .setDisableAutoCompactions(true);
    }

    ColumnFamilyOptions prefixColumnFamilyOptions() {
        return new ColumnFamilyOptions()
                .setMaxWriteBufferNumber(memtableNumber)
                .setNumLevels(2)
                .setWriteBufferSize(memtableSize)
                .setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setIndexType(IndexType.kHashSearch)
                        .setFilter(new BloomFilter(bloomBits, false))
                        .setWholeKeyFiltering(false));
    }

    WriteOptions writeOptions() {
        return new WriteOptions().setDisableWAL(true).setNoSlowdown(true);
    }

    ReadOptions iteratorOptions() {
        return new ReadOptions().setTotalOrderSeek(true);
    }

    ReadOptions prefixIteratorOptions() {
        return new ReadOptions().setPrefixSameAsStart(true);
    }

    FlushOptions flushOptions() {
        return new FlushOptions().setWaitForFlush(true);
    }


}
