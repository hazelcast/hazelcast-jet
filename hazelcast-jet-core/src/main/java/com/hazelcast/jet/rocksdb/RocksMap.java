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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to processors.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class RocksMap<K, V> implements Iterable<Entry<K, V>> {
    private final RocksDB db;
    private final InternalSerializationService serializationService;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final ColumnFamilyHandle cfh;
    private final ArrayList<RocksMapIterator> iterators = new ArrayList<>();

    RocksMap(@Nonnull RocksDB db, @Nonnull String name, @Nonnull RocksDBOptions options,
             @Nonnull InternalSerializationService serializationService) {
        this.db = db;
        this.serializationService = serializationService;
        columnFamilyOptions = options.columnFamilyOptions();
        writeOptions = options.writeOptions();
        readOptions = options.readOptions();
        try {
            cfh = db.createColumnFamily(new ColumnFamilyDescriptor(serialize(name), columnFamilyOptions));
        } catch (RocksDBException e) {
            throw new JetException("Failed to create RocksMap", e);
        }
    }

    /**
     * Returns the value mapped to a given key.
     *
     * @param key the key whose value is to be returned
     * @return the value associated with key or null if the key doesn't exist
     * @throws JetException if the database is closed
     */

    public V get(Object key) throws JetException {
        try {
            byte[] valueBytes = db.get(cfh, readOptions, serialize(key));
            return deserialize(valueBytes);
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Get", e);
        }
    }

    /**
     * Maps a key to a value.
     * If the key is already present, it updates the current value.
     *
     * @param key the key whose value is to be updated
     * @throws JetException if the database is closed
     */
    public void put(K key, V value) throws JetException {
        try {
            db.put(cfh, writeOptions, serialize(key), serialize(value));
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Put", e);
        }
    }

    /**
     * Deletes the mapping between key and value.
     *
     * @param key the key whose value is to be removed
     * @throws JetException if the database is closed
     */
    public void remove(K key) throws JetException {
        try {
            db.delete(cfh, writeOptions, serialize(key));
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Delete", e);
        }
    }

    /**
     * Returns all key-value mappings in this map.
     *
     * @return a HashMap containing all key-value pairs
     * @throws JetException if the database is closed
     */
    public Map<K, V> getAll() {
        Map<K, V> map = new HashMap<>();
        for (Entry<K, V> entry : this) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    @Nonnull
    @Override
    public Iterator<Entry<K, V>> iterator() {
        RocksMapIterator iterator = new RocksMapIterator();
        iterators.add(iterator);
        return iterator;
    }

    /**
     * Releases all native handles that this map acquires.
     */
    void close() {
        writeOptions.close();
        readOptions.close();
        columnFamilyOptions.close();
        iterators.forEach(RocksMapIterator::close);
        cfh.close();
    }

    private <T> byte[] serialize(T item) {
        if (item == null) {
            return null;
        }
        ObjectDataOutput out = serializationService.createObjectDataOutput();
        serializationService.writeObject(out, item);
        return out.toByteArray();
    }

    private <T> T deserialize(byte[] item) {
        if (item == null) {
            return null;
        }
        ObjectDataInput in = serializationService.createObjectDataInput(item);
        return serializationService.readObject(in);
    }

    private class RocksMapIterator implements Iterator<Entry<K, V>> {
        private final RocksIterator iterator;

        RocksMapIterator() {
            iterator = db.newIterator(cfh, readOptions);
            iterator.seekToFirst();
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Entry<K, V> next() {
            Tuple2<K, V> tuple = tuple2(deserialize(iterator.key()), deserialize(iterator.value()));
            iterator.next();
            return tuple;
        }

        @Override
        public void remove() {
        }

        void close() {
            iterator.close();
        }
    }
}
