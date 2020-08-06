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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.InternalSerializationService;
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
import java.util.Map.Entry;

import static com.hazelcast.jet.rocksdb.Tuple2.tuple2;

/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to processors.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class RocksMap<K, V> {
    private final RocksDB db;
    private final InternalSerializationService serializationService;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final ColumnFamilyHandle cfh;
    private final ArrayList<RocksIterator> iterators = new ArrayList<>();

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
            throw new HazelcastException("Failed to create RocksMap", e);
        }
    }

    /**
     * Returns the value mapped to a given key.
     *
     * @param key the key whose value is to be returned
     * @return the value associated with key or null if the key doesn't exist
     * @throws HazelcastException if the database is closed
     */

    public V get(K key) throws HazelcastException {
        try {
            byte[] valueBytes = db.get(cfh, readOptions, serialize(key));
            return deserialize(valueBytes);
        } catch (RocksDBException e) {
            throw new HazelcastException("Operation Failed: Get", e);
        }
    }

    /**
     * Maps a key to a value.
     * If the key is already present, it updates the current value.
     *
     * @param key the key whose value is to be updated
     * @throws HazelcastException if the database is closed
     */
    public void put(K key, V value) throws HazelcastException {
        try {
            db.put(cfh, writeOptions, serialize(key), serialize(value));
        } catch (RocksDBException e) {
            throw new HazelcastException("Operation Failed: Put", e);
        }
    }

    /**
     * Deletes the mapping between key and value.
     *
     * @param key the key whose value is to be removed
     * @throws HazelcastException if the database is closed
     */
    public void remove(K key) throws HazelcastException {
        try {
            db.delete(cfh, writeOptions, serialize(key));
        } catch (RocksDBException e) {
            throw new HazelcastException("Operation Failed: Delete", e);
        }
    }

    /**
     * Returns an iterator over the contents of this map.
     */
    @Nonnull
    public RocksMapIterator iterator() {
        RocksMapIterator mapIterator = new RocksMapIterator();
        iterators.add(mapIterator.iterator);
        return mapIterator;
    }

    /**
     * Releases all native handles that this map acquires.
     */
    void close() {
        writeOptions.close();
        readOptions.close();
        columnFamilyOptions.close();
        for (RocksIterator iterator : iterators) {
            iterator.close();
        }
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

    /**
     * Iterator over the entries in RocksMap.
     * The iterator creates a snapshot of the map when it is created,
     * any update applied after the iterator is created can't be seen by the iterator.
     * Callers have to invoke close() at the end to release the associated native iterator.
     */
    public final class RocksMapIterator {
        private final RocksIterator iterator;

        private RocksMapIterator() {
            iterator = db.newIterator(cfh, readOptions);
            iterator.seekToFirst();
        }

        /**
         * Returns whether the iterator has more entries to traverse.
         */
        public boolean hasNext() {
            return iterator.isValid();
        }

        /**
         * Returns the next entry in the map.
         */
        public Entry<K, V> next() {
            Tuple2<K, V> tuple = tuple2(deserialize(iterator.key()), deserialize(iterator.value()));
            iterator.next();
            return tuple;
        }

        /**
         * Removes the current entry from the map.
         */
        //TODO: decide whether implement the method since iterator uses a snapshot of the database.
        public void remove() {
        }

        /**
         * Releases the native RocksDB iterator used by this iterator.
         */
        public void close() {
            iterator.close();
            iterators.remove(iterator);
        }
    }
}
