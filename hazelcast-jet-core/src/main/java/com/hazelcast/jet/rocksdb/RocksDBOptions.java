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

import org.rocksdb.Options;

/**
 * A configuration class where the RocksDB default options should be placed.
 * Used by RocksDBFactory to Create RocksDBStateBackend.
 * Note : RocksDB.Options extends DBOptions and ColumnFamilyOptions
 * it can be used to configure database-wide and column family options
 */
class RocksDBOptions {
    Options getOptions() {
        return new Options();
    }
}
