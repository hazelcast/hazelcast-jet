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
import org.rocksdb.ColumnFamilyHandle;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to tasks.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class RocksMap<K, V> {
    public RocksMap(RocksDB db, ColumnFamilyHandle cfh) {
    }

    public V get(K key) {
        return null;
    }

    public void put(K key, V value) {
    }

    public Map<K, V> getAll() {
        return null;
    }

    public void putAll(Map<K, V> entries) {
    }

    public void delete(K key) {
    }

    public Iterator<Map.Entry<K, V>> all() {
        return null;
    }

    //We may choose to implement this method
    //relies on rocksdb merge operator. see : https://github.com/facebook/rocksdb/wiki/Merge-Operator
    //instead of getting the list in the value
    //modifying it, then writing it back, just merge the value in one step
    public void merge(K key, V value, BiFunction fn) {
    }

}
