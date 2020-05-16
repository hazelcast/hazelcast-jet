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

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;

/**
 * Responsible for managing one RocksDB instance,
 * opening and Closing the connection and deleting the database.
 * Processors acquire an instance of this class on initialization.
 * Processors use this class to acquire any number of RocksMaps they require.
 * The datastore is logically partitioned using column families.
 * Each RocksMap is instantiated with a ColumnFamilyHandler.
 * Once the task has finished (complete() is invoked),
 * the task asks it to delete the whole data-store.
 */

public class RocksDBStateBackend {
    private RocksDB db;
    private ArrayList<ColumnFamilyHandle> cfhs = new ArrayList<>();

    RocksDBStateBackend(Options opt, String directory) {
        RocksDB.loadLibrary();
        try {
            db = RocksDB.open(opt, directory);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public <K,V> RocksMap getMap(Class<K> k ,Class<V> v) {
        ColumnFamilyHandle cfh = null;
        try {
            cfh = db.createColumnFamily(new ColumnFamilyDescriptor("RocksMap1".getBytes()));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        cfhs.add(cfh);
        return new RocksMap<K,V>(db, cfh,k,v);
    }

    public void deleteDataStore() {
        for (final ColumnFamilyHandle cfh : cfhs) {
            cfh.close();
        }
    }
}
