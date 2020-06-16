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

class RocksDBOptions {
    private static final int MEMTABLE_SIZE = 64 * 1024 * 1024;
    private static final int MEMTABLE_NUMBER = 2;
    private static final int MEMTABLES_SEALED = 0;
    private static final int LEVELS = 2;
    private static final int FLUSHES = 2;
    private static final int BLOOM_BITS = 10;

    //TODO: make options configurable and separate prefix and regular options
    Options options() {
        return new Options()
                .setCreateIfMissing(true)
                .setMaxBackgroundFlushes(FLUSHES)
                // TODO: can't split into two modes, needs to be set when db is opened
                .prepareForBulkLoad()
                .setAllowConcurrentMemtableWrite(false)
                .setDisableAutoCompactions(true);
    }

    ColumnFamilyOptions columnFamilyOptions() {
        return new ColumnFamilyOptions();
    }

    ColumnFamilyOptions prefixColumnFamilyOptions() {
        return new ColumnFamilyOptions()
                .setMaxWriteBufferNumber(MEMTABLE_NUMBER)
                .setMaxWriteBufferNumberToMaintain(MEMTABLES_SEALED)
                .setNumLevels(LEVELS)
                .setWriteBufferSize(MEMTABLE_SIZE)
                .setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setIndexType(IndexType.kHashSearch)
                        .setFilter(new BloomFilter(BLOOM_BITS, false))
                        .setWholeKeyFiltering(false));
    }

    WriteOptions writeOptions() {
        return new WriteOptions().setDisableWAL(true);
    }

    ReadOptions readOptions() {
        return new ReadOptions();
    }

    public ReadOptions iteratorOptions() {
        return new ReadOptions().setTotalOrderSeek(true);
    }

    public ReadOptions prefixIteratorOptions() {
        return new ReadOptions().setPrefixSameAsStart(true);
    }
}
