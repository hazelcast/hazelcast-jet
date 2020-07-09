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

/**
 * General RocksDB Configurations
 */
class RocksDBOptions {
    private static final int MEMTABLE_SIZE = 64 * 1024 * 1024;
    private static final int MEMTABLE_NUMBER = 4;
    private static final int BLOOM_BITS = 10;
    private static final int CACHE_SIZE = 265 * 1024 * 1024;


    Options options() {
        return new Options().setCreateIfMissing(true);
    }

    ColumnFamilyOptions columnFamilyOptions() {
        return new ColumnFamilyOptions()
                .setMaxWriteBufferNumber(MEMTABLE_NUMBER)
                .setWriteBufferSize(MEMTABLE_SIZE)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockCacheSize(CACHE_SIZE)
                        .setPinL0FilterAndIndexBlocksInCache(true)
                        .setFilter(new BloomFilter(BLOOM_BITS)));
    }

    WriteOptions writeOptions() {
        return new WriteOptions().setDisableWAL(true).setNoSlowdown(true);
    }

    ReadOptions readOptions() {
        return new ReadOptions();
    }
}
