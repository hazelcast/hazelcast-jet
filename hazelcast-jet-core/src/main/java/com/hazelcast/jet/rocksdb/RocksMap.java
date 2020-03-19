package com.hazelcast.jet.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.ColumnFamilyHandle;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to tasks.
 */
public class RocksMap<K, V>  {
    public RocksMap(RocksDB db, ColumnFamilyHandle cfh) { }
    public V get(K key) {
        return null;
    }
    public void put(K key, V value) { }
    public Map<K, V> getAll() {
        return null;
    }
    public void putAll(Map<K, V> entries) { }
    public void delete(K key) { }
    public Iterator<Map.Entry<K, V>> all() {
        return null;
    }

    //We may choose to implement this method
    //relies on rocksdb merge operator. see : https://github.com/facebook/rocksdb/wiki/Merge-Operator
    //instead of getting the list in the value
    //modifying it, then writing it back, just merge the value in one step
    public void merge(K key, V value, BiFunction fn) { }

}
