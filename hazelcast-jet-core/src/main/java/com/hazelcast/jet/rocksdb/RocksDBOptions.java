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
 * Contains RocksDB configurations suitable for bulk-loading.
 */
public class RocksDBOptions {
    Options options() {
        return new Options()
                .setCreateIfMissing(true)
                .prepareForBulkLoad()
                .setMaxBackgroundFlushes(2)
                .setAllowConcurrentMemtableWrite(false)
                .setDisableAutoCompactions(true);
    }

    public ColumnFamilyOptions columnFamilyOptions() {
        return new ColumnFamilyOptions()
                .setMaxWriteBufferNumber(4)
                .setMaxWriteBufferNumberToMaintain(0)
                .setNumLevels(2)
                .setWriteBufferSize(32 * 1024 * 1024)
                .setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setIndexType(IndexType.kHashSearch)
                        .setFilter(new BloomFilter(10, false))
                        .setWholeKeyFiltering(false));
    }

    ReadOptions readOptions() {
        return new ReadOptions();
    }

    WriteOptions writeOptions() {
        return new WriteOptions().setDisableWAL(true);
    }
}
