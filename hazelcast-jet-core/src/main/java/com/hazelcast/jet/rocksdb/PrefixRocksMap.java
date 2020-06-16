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
import java.util.Iterator;
import java.util.Map.Entry;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * Stores lists of values mapped to keys.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class PrefixRocksMap<K, V> implements Iterable<Entry<K, Iterator<V>>> {
    private final RocksDB db;
    private final String name;
    private final InternalSerializationService serializationService;
    private final RocksDBOptions options;
    private ColumnFamilyHandle cfh;
    private long counter = 0;

    PrefixRocksMap(RocksDB db, String name, RocksDBOptions options,
                   InternalSerializationService serializationService) {
        this.db = db;
        this.name = name;
        this.options = options;
        this.serializationService = serializationService;
    }

    //lazily creates the column family since the prefix is only specified once the map is actually used
    private void open(K prefix) {
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
     * adds the provided value in the list associated with the key.
     */
    public void add(K key, V value) throws JetException {
        if (cfh == null) {
            open(key);
        }
        try {
            db.put(cfh, writeOptions(), pack(key), serialize(value));
        } catch (Exception e) {
            throw new JetException("Operation Failed: add", e);
        }
    }

    /**
     * Retrieves all values associated with the given key.
     * Callers need to first acquire a native RocksDB iterator
     * by calling prefixRocksIterator() and keeping it for later calls.
     */
    public Iterator<V> get(RocksIterator iterator, K key) {
        if (cfh == null) {
            open(key);
        }
        iterator.seek(serialize(key));

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public V next() {
               V value =  deserialize(iterator.value());
                iterator.next();
                return value;
            }};
    }

    public void remove(K key) throws JetException {
        if (cfh == null) {
            open(key);
        }
        try {
            db.delete(cfh, writeOptions(), serialize(key));
        } catch (RocksDBException e) {
            throw new JetException("Operation Failed : remove ", e);
        }
    }

    /**
     * Used to acquire a native RocksDB iterator.
     * The returned iterator is used by callers to preform prefix reads and iteration.
     * Should be used with caution not to create to many native iterators.
     * Callers need to reuse the returned iterator.
     * Creating and iterator on each read or iteration will take much memory
     * and more likely cause the job to fail.
     */
    public RocksIterator prefixRocksIterator() {
        return db.newIterator(cfh, readOptions().setPrefixSameAsStart(true));
    }

    /**
     * Returns an iterator over the contents of this map.
     * This iterator is guaranteed to return all keys totally ordered.
     */
    @Nonnull
    @Override
    public Iterator<Entry<K, Iterator<V>>> iterator() {
        return new PrefixRocksMapIterator();
    }

    /**
     * Compacts RocksMap's ColumnFamily from level 0 to level 1.
     * This should be invoked to prepare RocksMap for reads after a series of prefixWrite().
     */
    public void compact() throws JetException {
        try {
            db.flush(new FlushOptions().setWaitForFlush(true), cfh);
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
            bytes.write(serialize(counter++));
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new JetException();
        }
    }

    private class PrefixRocksMapIterator implements Iterator<Entry<K, Iterator<V>>> {
        private final RocksIterator iterator;
        private final RocksIterator prefixIterator;

        PrefixRocksMapIterator() {
            iterator = db.newIterator(cfh, readOptions().setTotalOrderSeek(true));
            iterator.seekToFirst();
            prefixIterator = prefixRocksIterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Entry<K, Iterator<V>> next() {
            Tuple2<K, Iterator<V>> tuple = tuple2(deserialize(iterator.key()), get(prefixIterator, deserialize(iterator.key())));
            //skip over the current prefix
            K current = deserialize(iterator.key());
            while(deserialize(iterator.key()).equals(current) && iterator.isValid()) {
                iterator.next();
            }
            return tuple;
        }

        @Override
        public void remove() {
        }
    }
}
