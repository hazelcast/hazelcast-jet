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
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to processors.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class RocksMap<K, V> extends AbstractMap<K, V> {
    private final RocksDB db;
    private final ColumnFamilyHandle cfh;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final InternalSerializationService serializationService;
    private RocksMapIterator iterator;

    RocksMap(RocksDB db, ColumnFamilyHandle cfh,
             ReadOptions readOptions, WriteOptions writeOptions,
             InternalSerializationService serializationService) {
        this.db = db;
        this.cfh = cfh;
        this.readOptions = readOptions;
        this.writeOptions = writeOptions;
        this.serializationService = serializationService;
    }


    ColumnFamilyHandle getColumnFamilyHandle() {
        return cfh;
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
     * @return the old value mapped to the key or null
     * @throws JetException if the database is closed
     */
    //TODO: return the previous value instead of null
    public V put(K key, V value) throws JetException {
        try {
            db.put(cfh, writeOptions, serialize(key), serialize(value));
            return null;
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Put", e);
        }
    }

    /**
     * Copies all key-value mappings from the supplied map to this map.
     * makes use of RocksDB batch write feature.
     *
     * @param map the map containing key-value pairs to be inserted
     * @throws JetException if the database is closed
     */
    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> map) throws JetException {
        WriteBatch batch = new WriteBatch();
        for (Entry<? extends K, ? extends V> e : map.entrySet()) {
            try {
                batch.put(cfh, serialize(e.getKey()), serialize(e.getValue()));
            } catch (RocksDBException ex) {
                throw new JetException("Operation Failed: PutAll", ex);
            }
        }
        try {
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: PutAll", e);
        }
    }

    /**
     * Deletes the mapping between key and value.
     *
     * @param key the key whose value is to be removed
     * @throws JetException if the database is closed
     */
    //TODO: return the previous value instead of null
    @Override
    public V remove(Object key) throws JetException {
        try {
            db.delete(writeOptions, serialize(key));
            return null;
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Delete", e);
        }
    }

    /**
     * Returns all key-value mappings in the this map as in a HashMap
     *
     * @return a HashMap containing all key-value pairs
     * @throws JetException if the database is closed
     */
    public Map<K, V> getAll() {
        Map<K, V> map = new HashMap<>();
        for (Entry<K, V> entry : entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public void prefixWrite(K prefix, V key, V value) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            bytes.write(serialize(prefix));
            bytes.write(serialize(key));
            db.put(cfh, writeOptions, bytes.toByteArray(), serialize(value));
        } catch (Exception e) {
            throw new JetException("Operation Failed: prefixWrite", e);
        }
    }

    public Object prefixRead(K prefix) {
        ArrayList<V> values = new ArrayList<>();
        byte[] prefixBytes = serialize(prefix);
        RocksIterator iterator = db.newIterator(cfh, readOptions);
        for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(iterator.key());
            try {
                //TODO: get the byte size of each type
                byte[] bytes = inputStream.readNBytes(12);
                if (Arrays.equals(bytes, prefixBytes)) {
                    values.add(deserialize(iterator.value()));
                } else {
                    break;
                }
            } catch (IOException e) {
                throw new JetException("Operation Failed : perfixRead", e);
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        if (values.size() == 1) {
            return values.get(0);
        }
        iterator.close();
        return values;
    }

    /**
     * Compacts RocksMap's ColumnFamily from level 0 to level 1.
     * This should be invoked to prepare RocksMap for reads after bulk loading.
     */
    public void compact() throws JetException {
            try {
                db.flush(new FlushOptions() , cfh);
                db.compactRange(cfh);
            } catch (RocksDBException e) {
                throw new JetException("Failed to Compact RocksDB", e);
            }
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return new RocksMapSet();
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

    //TODO: (optional) implement remove
    // RocksIterator creates a snapshot of the database
    // but it's now used as an iterator over entry set which doesn't take a snapshot
    private class RocksMapIterator implements Iterator<Entry<K, V>> {
        RocksIterator iterator = db.newIterator(cfh, readOptions);

        RocksMapIterator() {
            iterator.seekToFirst();
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Entry<K, V> next() {
            K key = deserialize(iterator.key());
            V value = deserialize(iterator.value());
            Tuple2<K, V> tuple = tuple2(key, value);
            iterator.next();
            return tuple;
        }

        @Override
        public void remove() {
        }
    }


    private class RocksMapSet extends AbstractSet<Entry<K, V>> {
        @Nonnull
        @Override
        public Iterator<Entry<K, V>> iterator() {
            if (iterator == null) {
                iterator = new RocksMapIterator();
            }
            return iterator;
        }

        //TODO: we need to keep the size of the entry set
        // put and delete doesn't necessarily change size
        // for example, updating a key or deleting a key that doesn't exists
        @Override
        public int size() {
            return 0;
        }
    }
}
