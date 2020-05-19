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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.datamodel.Tuple2;
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
 */
public class RocksMap implements Iterable<Entry<byte[], byte[]>> {
    private final RocksDB db;
    private final ColumnFamilyHandle cfh;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;

    RocksMap(RocksDB db, ColumnFamilyHandle cfh, ReadOptions readOptions, WriteOptions writeOptions) {
        this.db = db;
        this.cfh = cfh;
        this.readOptions = readOptions;
        this.writeOptions = writeOptions;
    }

    public byte[] get(byte[] key) throws JetException {
        try {
            return db.get(cfh, readOptions, key);
        } catch (RocksDBException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    public void put(byte[] key, byte[] value) throws JetException {
        try {
            db.put(cfh, writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    public void delete(byte[] key) throws JetException {
        try {
            db.delete(writeOptions, key);
        } catch (RocksDBException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    public void putAll(@Nonnull Map<byte[], byte[]> map) throws JetException {
        WriteBatch batch = new WriteBatch();
        for (Entry<byte[], byte[]> e : map.entrySet()) {
            batch.put(e.getKey(), e.getValue());
        }
        try {
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    public Map<byte[], byte[]> getAll() {
        Map<byte[], byte[]> map = new HashMap<>();
        for (Entry<byte[], byte[]> kv : this) {
            map.put(kv.getKey(), kv.getValue());
        }
        return map;
    }

    @Nonnull
    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
        return new RocksMapIterator();
    }

    private class RocksMapIterator implements Iterator<Entry<byte[], byte[]>> {
        RocksIterator iterator = db.newIterator(cfh, readOptions);

        RocksMapIterator() {
            iterator.seekToFirst();
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Entry<byte[], byte[]> next() {
            Tuple2<byte[], byte[]> tuple = tuple2(iterator.key(), iterator.value());
            iterator.next();
            return tuple;
        }
    }
}
