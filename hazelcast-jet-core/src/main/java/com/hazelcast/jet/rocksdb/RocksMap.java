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
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

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
    private final String name;
    private final InternalSerializationService serializationService;
    private final RocksDBOptions options;
    private ColumnFamilyHandle cfh;

    RocksMap(RocksDB db, String name, RocksDBOptions options,
             InternalSerializationService serializationService) {
        this.db = db;
        this.name = name;
        this.options = options;
        this.serializationService = serializationService;
    }

    //lazily creates the column family since the prefix is only specified once the map is actually used
    //only prefix operations will work for now.
    private void open(K prefix) throws JetException {
        try {
            cfh = db.createColumnFamily(new ColumnFamilyDescriptor(serialize(name),
                    columnFamilyOptions().useFixedLengthPrefixExtractor(serialize(prefix).length)));
        } catch (RocksDBException e) {
            throw new JetException("Failed to create RocksMap", e);
        }
    }

    private WriteOptions writeOptions() {
        return options.writeOptions();
    }

    private ReadOptions readOptions() {
        return options.readOptions();
    }

    private ColumnFamilyOptions columnFamilyOptions() {
        return options.columnFamilyOptions();
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
            byte[] valueBytes = db.get(cfh, readOptions(), serialize(key));
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
            db.put(cfh, writeOptions(), serialize(key), serialize(value));
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
            db.delete(cfh, writeOptions(), serialize(key));
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

    public void prefixWrite(K prefix, V value) {
        if (cfh == null) {
            open(prefix);
        }
        try {

            db.put(cfh, writeOptions(), pack(prefix), serialize(value));
        } catch (Exception e) {
            throw new JetException("Operation Failed: prefixWrite", e);
        }
    }

    //this is used to reserve an iterator for the joiner processor.
    public RocksIterator prefixRocksIterator() {
        return db.newIterator(cfh, readOptions().setPrefixSameAsStart(true));
    }

    /**
     * Retrieves all values associated with a given prefix.
     *
     * @param iterator the result of createPrefixIterator()
     */
    public Object prefixRead(RocksIterator iterator, K prefix) {
        if (cfh == null) {
            open(prefix);
        }
        ArrayList<V> values = new ArrayList<>();
        for (iterator.seek(serialize(prefix)); iterator.isValid(); iterator.next()) {
            values.add(deserialize(iterator.value()));
        }
        if (values.isEmpty()) {
            return null;
        }
        if (values.size() == 1) {
            return values.get(0);
        }
        return values;
    }

    public Iterator<V> prefixRead(K prefix) {
        if (cfh == null) {
            open(prefix);
        }
        return new PrefixIterator(prefix);
    }

    /**
     * Compacts RocksMap's ColumnFamily from level 0 to level 1.
     * This should be invoked to prepare RocksMap for reads after bulk loading.
     */
    public void compact() throws JetException {
        try {
            db.flush(new FlushOptions(), cfh);
            db.compactRange(cfh);
        } catch (RocksDBException e) {
            throw new JetException("Failed to Compact RocksDB", e);
        }
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

    private <T> byte[] pack(T item) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            bytes.write(serialize(item));
            bytes.write(serialize(UUID.randomUUID()));
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new JetException();
        }
    }

    private class PrefixIterator implements Iterator<V> {
        private final RocksIterator iterator;

        PrefixIterator(K prefix) {
            iterator = db.newIterator(cfh, readOptions().setPrefixSameAsStart(true));
            iterator.seek(serialize(prefix));
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public V next() {
            V result = deserialize(iterator.value());
            iterator.next();
            return result;
        }
    }

    @Nonnull
    @Override
    public Iterator<Entry<K, V>> iterator() {
        return new RocksMapIterator();
    }

    private class RocksMapIterator implements Iterator<Entry<K, V>> {
        private final RocksIterator iterator;

        RocksMapIterator() {
            iterator = db.newIterator(cfh, readOptions().setTotalOrderSeek(true));
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
    }
}
