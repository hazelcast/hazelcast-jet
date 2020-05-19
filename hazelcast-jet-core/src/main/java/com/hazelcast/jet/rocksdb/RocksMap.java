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
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to processors.
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class RocksMap<K, V> implements Iterable<Entry<K, V>> {
    private final RocksDB db;
    private final ColumnFamilyHandle cfh;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final InternalSerializationService serializationService;

    RocksMap(RocksDB db, ColumnFamilyHandle cfh,
             ReadOptions readOptions, WriteOptions writeOptions,
             InternalSerializationService serializationService) {
        this.db = db;
        this.cfh = cfh;
        this.readOptions = readOptions;
        this.writeOptions = writeOptions;
        this.serializationService = serializationService;

    }

    public V get(K key) throws JetException {
        try {
            byte[] valueBytes = db.get(cfh, readOptions, serialize(key));
            return deserialize(valueBytes);
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Get", e);
        }
    }

    public void put(K key, V value) throws JetException {
        try {
            db.put(cfh, writeOptions, serialize(key), serialize(value));
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Put", e);
        }
    }

    public void delete(K key) throws JetException {
        try {
            db.delete(writeOptions, serialize(key));
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: Delete", e);
        }
    }

    public void putAll(@Nonnull Map<K, V> map) throws JetException {
        WriteBatch batch = new WriteBatch();
        for (Entry<K, V> e : map.entrySet()) {
            byte[] keyBytes = serialize(e.getKey());
            byte[] valueBytes = serialize(e.getValue());
            batch.put(keyBytes, valueBytes);
        }
        try {
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed: PutAll", e);
        }
    }

    public Map<K, V> getAll() {
        Map<K, V> map = new HashMap<>();
        for (Entry<K, V> entry : this) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    private <T> byte[] serialize(T item) {
        ObjectDataOutput out = serializationService.createObjectDataOutput();
        serializationService.writeObject(out, item);
        return out.toByteArray();
    }

    private <T> T deserialize(byte[] item) {
        ObjectDataInput in = serializationService.createObjectDataInput(item);
        return serializationService.readObject(in);
    }

    @Nonnull
    @Override
    public Iterator<Entry<K, V>> iterator() {
        return new RocksMapIterator();
    }

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
    }
}
