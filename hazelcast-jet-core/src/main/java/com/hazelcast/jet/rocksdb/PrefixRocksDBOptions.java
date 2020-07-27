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
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * RocksDB configuration optimized for sequential access pattern using prefix-mode.
 * Prefix mode consists of a bulk-loading phase followed by a prefix-scan phase.
 * Bulk-loading uses a Vector Memtable to make a series of inserts for a set of values under some key.
 * The key is serialized and packed with a unique long value before insertion.
 * After bulk-loading is done, the database is compacted then the prefix-scan phase begins.
 * Prefix-scan retrieves all values associated with each key using the key as the prefix.
 * Prefix-scan uses RocksDB prefix iterator feature with a fixed-length prefix equal to the size of PrefixRocksMap key.
 */
public class PrefixRocksDBOptions extends RocksDBOptions implements Serializable {

    private static final int MEMTABLE_SIZE = 128 * 1024 * 1024;
    private static final int MEMTABLE_NUMBER = 4;
    private static final int BLOOM_FILTER_BITS = 10;
    private static final int NUM_LEVELS = 2;
    private static final int SUB_COMPACTIONS = 4;
    private int memtableSize;
    private int memtableNumber;
    private int bloomFilterBits;
    private int subCompactions;

    /**
     * Creates a new PrefixRocksDBOptions instance with default options.
     */
    public PrefixRocksDBOptions() {
        memtableSize = MEMTABLE_SIZE;
        memtableNumber = MEMTABLE_NUMBER;
        bloomFilterBits = BLOOM_FILTER_BITS;
        subCompactions = SUB_COMPACTIONS;
    }

    // need it to make a copy of this instance so a new instance with the same
    // configuration is passed to each new map.
    PrefixRocksDBOptions(@Nonnull PrefixRocksDBOptions options) {
        memtableSize = options.memtableSize;
        memtableNumber = options.memtableNumber;
        bloomFilterBits = options.bloomFilterBits;
        subCompactions = options.subCompactions;
    }
    /**
     * Sets RocksDB options using the builder instance the user provided through JobConfig.
     */
    public PrefixRocksDBOptions setOptions(@Nonnull PrefixRocksDBOptionsBuilder options) {
        if (options.memtableSize != null) {
            memtableSize = options.memtableSize;
        }
        if (options.memtableNumber != null) {
            memtableNumber = options.memtableNumber;
        }
        if (options.bloomFilterBits != null) {
            bloomFilterBits = options.bloomFilterBits;
        }
        if (options.subCompactions != null) {
            subCompactions = options.subCompactions;
        }
        return this;
    }

    @Override
    Options options() {
        Options options = new Options()
                .setCreateIfMissing(true)
                .prepareForBulkLoad()
                .setAllowConcurrentMemtableWrite(false);
        options.setMaxSubcompactions(subCompactions);
        return options;
    }

    @Override
    ColumnFamilyOptions columnFamilyOptions() {
        return new ColumnFamilyOptions()
                .setNumLevels(NUM_LEVELS)
                .setMaxWriteBufferNumber(memtableNumber)
                .setWriteBufferSize(memtableSize)
                .setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setIndexType(IndexType.kHashSearch)
                        .setFilter(new BloomFilter(bloomFilterBits, false))
                        .setWholeKeyFiltering(false));
    }

    @Override
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

    @Override
    void close() {
    }
}
