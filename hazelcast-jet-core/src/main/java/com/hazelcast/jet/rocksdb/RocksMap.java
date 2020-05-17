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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to processors.
 */
public class RocksMap {
    private final RocksDB db;
    private final ColumnFamilyHandle cfh;

    RocksMap(RocksDB db, ColumnFamilyHandle cfh) {
        this.db = db;
        this.cfh = cfh;
    }

    public byte[] get(byte[] key) {
        try {
            return db.get(cfh, key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(byte[] key, byte[] value) {
        try {
            db.put(cfh, key, value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void delete(byte[] key) {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void putAll(Map<byte[], byte[]> map) {
        for (Entry<byte[], byte[]> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    public Map<byte[], byte[]> getAll() {
        return null;
    }

    public Iterator<Entry<byte[], byte[]>> all() {
        return null;
    }
}
