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
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.WriteOptions;

/**
 * A configuration class where the RocksDB default options should be placed.
 * Used by RocksDBFactory to Create RocksDBStateBackend.
 */
class RocksDBOptions {
    private static final Integer FLUSHES = 2;
    private static final Integer CACHE_SIZE = 16 * 1024;
    //TODO: set the byte size for each type.
    private static final Integer LONG_BYTES = 12;

    Options getOptions() {
        //recommended options for general workload
        // see: https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
        return new Options()
                .setCreateIfMissing(true)
                .prepareForBulkLoad()
                .setMaxBackgroundFlushes(FLUSHES)
                .useFixedLengthPrefixExtractor(LONG_BYTES)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setIndexType(IndexType.kHashSearch)
                        .setFilter(new BloomFilter(10, false))
                        .setWholeKeyFiltering(false)
                        .setBlockCacheSize(CACHE_SIZE));
    }

    ReadOptions getReadOptions() {
        return new ReadOptions();
    }

    WriteOptions getWriteOptions() {
        return new WriteOptions()
                //bypass RocksDB write-ahead-log since the state backend is considered volatile
                .setDisableWAL(true);
    }
}
