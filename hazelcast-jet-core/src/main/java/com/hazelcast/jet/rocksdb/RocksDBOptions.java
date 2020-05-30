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
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.WriteOptions;

/**
 * A configuration class where the RocksDB default options should be placed.
 * Used by RocksDBFactory to Create RocksDBStateBackend.
 * Note : RocksDB.Options extends DBOptions and ColumnFamilyOptions
 * it can be used to configure database-wide and column family options
 */
class RocksDBOptions {
    Options getOptions() {
        return new Options()
                // we're opening a new RocksDB instance
                .setCreateIfMissing(true)
                // sets up RocksDB block cache with default configs
                .setTableFormatConfig(new BlockBasedTableConfig()
                        // speedup Get() by maintaining a bloom filter for each on-disk sst file
                        // this should cause significant gain since our use case is large state that doesn't fit in memory
                        .setFilter(new BloomFilter()))
                // bypass OS page-cache
                // avoids copying data twice from storage to page-cache and then to RocksDB block-cache
                // RocksDB is likely to have more knowledge about its access pattern than OS
                // this also enables read-ahead optimization for iterators for full range scans.
                .setUseDirectReads(true)
                .setUseDirectIoForFlushAndCompaction(true);
    }

    ReadOptions getReadOptions() {
        return new ReadOptions()
                //iterator is used for the scan at the end, no need to keep its data
                .setPinData(false);
    }

    WriteOptions getWriteOptions() {
        return new WriteOptions()
                //bypass RocksDB write-ahead-log since the state backend is considered volatile
                .setDisableWAL(true)
                //ignore any write that occurred after the ColumnFamily is closed
                .setIgnoreMissingColumnFamilies(true);
    }
}
