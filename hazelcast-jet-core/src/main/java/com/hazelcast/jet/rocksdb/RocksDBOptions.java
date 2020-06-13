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
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.WriteOptions;

/**
 * A configuration class where the RocksDB default options should be placed.
 * Used by RocksDBFactory to Create RocksDBStateBackend.
 */
class RocksDBOptions {
    private static final int FLUSHES = 2;
    private Integer prefixLength;

    Options getOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .prepareForBulkLoad()
                .setMaxBackgroundFlushes(FLUSHES)
                .setAllowConcurrentMemtableWrite(false);
    }

    public ColumnFamilyOptions getCFOptions() {
        assert prefixLength != null : "prefix not set";
        return new ColumnFamilyOptions()
                .setMemTableConfig(new VectorMemTableConfig())
                .useFixedLengthPrefixExtractor(prefixLength)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setIndexType(IndexType.kHashSearch)
                        .setFilter(new BloomFilter(10, false))
                        .setWholeKeyFiltering(false));
    }

    RocksDBOptions setPrefix(int prefix) {
        prefixLength = prefix;
        return this;
    }

    ReadOptions getReadOptions() {
        return new ReadOptions();
    }

    WriteOptions getWriteOptions() {
        return new WriteOptions().setDisableWAL(true);
    }
}
