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
import org.rocksdb.Status.Code;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * A RocksDB-backed Map that stores lists of values mapped to keys.
 * This Map makes use of RocksDB bulk-loading and prefix iteration features.
 * Lifecycle:
 * <ol><li>
 *     A processor acquire an instance of this class using
 *     {@link PrefixRocksDBStateBackend#getPrefixMap}.
 * </li><li>
 *     The processor issues a series of add() operations to load keys and
 *     values then call compact() to prepare the map for reads.
 * </li><li>
 *     The processor acquires one or more iterators using {@link #prefixRocksIterator}.
 * </li><li>
 *     The processor can issue a series of get() operations using the iterators
 *     it acquired to retrieve the values in the map.
 * </li><li>
 * The processor calls close() to release all memory this map owns.
 * </li></ol>
 * <p>
 * Notes:
 * <ol><li>
 * get() operations on this map are thread-safe however add() operations are not.
 * </li><li>
 * Not calling close() after execution completes will cause a memory leak.
 * </li></ol>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class PrefixRocksMap<K, V> {
    private final RocksDB db;
    private final String name;
    private final InternalSerializationService serializationService;
    private final ArrayList<RocksIterator> iterators = new ArrayList<>();
    private final ColumnFamilyOptions columnFamilyOptions;
    private final WriteOptions writeOptions;
    private final ReadOptions prefixIteratorOptions;
    private final ReadOptions iteratorOptions;
    private final FlushOptions flushOptions;
    private ColumnFamilyHandle cfh;
    private long counter = Long.MIN_VALUE;


    PrefixRocksMap(@Nonnull RocksDB db, @Nonnull String name, @Nonnull PrefixRocksDBOptions options,
                   @Nonnull InternalSerializationService serializationService) {
        this.db = db;
        this.name = name;
        this.serializationService = serializationService;
        columnFamilyOptions = options.columnFamilyOptions();
        writeOptions = options.writeOptions();
        prefixIteratorOptions = options.prefixIteratorOptions();
        iteratorOptions = options.iteratorOptions();
        flushOptions = options.flushOptions();
    }

    // lazily creates the column family since the prefix is only specified once
    // the map is actually used
    private void open(K prefix) {
        try {
            cfh = db.createColumnFamily(new ColumnFamilyDescriptor(serialize(name),
                    columnFamilyOptions.useFixedLengthPrefixExtractor(serialize(prefix).length)));
        } catch (RocksDBException e) {
            throw new JetException("Failed to create PrefixRocksMap", e);
        }
    }

    /**
     * Adds the provided value in the list associated with the key.
     * Returns true if the write request succeeded, false if a write stall occurred.
     */
    public boolean add(K key, V value) throws JetException {
        if (cfh == null) {
            open(key);
        }
        try {
            db.put(cfh, writeOptions, pack(key), serialize(value));
        } catch (RocksDBException e) {
            if (e.getStatus().getCode() == Code.Incomplete) {
                return false;
            }
            throw new JetException("Operation Failed: add", e);
        }
        return true;
    }

    /**
     * Retrieves all values associated with the given key. Callers need to
     * first acquire a native RocksDB iterator by calling {@link
     * #prefixRocksIterator} and keeping it for later calls.
     *
     * @return an iterator over the values associated with the key.
     */
    public Iterator<V> get(RocksIterator iterator, K key) {
        if (cfh == null) {
            open(key);
        }
        iterator.seek(serialize(key));

        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public V next() {
                V value = deserialize(iterator.value());
                iterator.next();
                return value;
            }
        };
    }

    /**
     * Used to acquire a native RocksDB iterator. The returned iterator is used
     * by callers to preform prefix reads and iteration. Should be used with
     * caution not to create to many native iterators. Callers need to reuse
     * the returned iterator. Otherwise, creating and iterator on each read
     * will take too much memory and cause the job to fail.
     */
    public RocksIterator prefixRocksIterator() {
        assert cfh != null : "PrefixRocksMap was not opened";
        RocksIterator rocksIterator = db.newIterator(cfh, prefixIteratorOptions);
        iterators.add(rocksIterator);
        return rocksIterator;
    }

    /**
     * Returns an iterator over the contents of this map. This iterator is
     * guaranteed to return all keys totally ordered.
     */
    @Nonnull
    public PrefixRocksMapIterator iterator() {
        assert cfh != null : "PrefixRocksMap was not opened";
        PrefixRocksMapIterator mapIterator = new PrefixRocksMapIterator();
        iterators.add(mapIterator.iterator);
        return mapIterator;
    }

    /**
     * Compacts RocksMap's ColumnFamily from level 0 to level 1. This should be
     * invoked to prepare RocksMap for reads after bulk-loading with a series of
     * add() calls.
     */
    public void compact() throws JetException {
        if (cfh != null) {
            try {
                db.flush(flushOptions, cfh);
                db.compactRange(cfh);
            } catch (RocksDBException e) {
                throw new JetException("Failed to Compact RocksDB", e);
            }
        }
    }

    /**
     * Releases all native handles that this map acquired.
     */
    void close() {
        columnFamilyOptions.close();
        writeOptions.close();
        iteratorOptions.close();
        prefixIteratorOptions.close();
        flushOptions.close();
        if (cfh != null) {
            cfh.close();
            for (RocksIterator rocksIterator : iterators) {
                rocksIterator.close();
            }
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

    @Nonnull
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

    /**
     * Iterator over the entries in PrefixRocksMap.
     * The iterator creates a snapshot of the map when it is created,
     * any update applied after the iterator is created can't be seen by the iterator.
     * Callers have to invoke close() at the end to release the associated native iterator.
     */
    public final class PrefixRocksMapIterator {
        private final RocksIterator iterator;
        private final RocksIterator prefixIterator;

        private PrefixRocksMapIterator() {
            iterator = db.newIterator(cfh, iteratorOptions);
            iterator.seekToFirst();
            prefixIterator = prefixRocksIterator();
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
        public Entry<K, Iterator<V>> next() {
            Tuple2<K, Iterator<V>> tuple = tuple2(deserialize(iterator.key()),
                    get(prefixIterator, deserialize(iterator.key())));
            //skip over the current prefix
            K current = deserialize(iterator.key());
            while (deserialize(iterator.key()).equals(current) && iterator.isValid()) {
                iterator.next();
            }
            return tuple;
        }

        /**
         * Releases the native RocksDB iterator used by this iterator.
         */
        public void close() {
            iterator.close();
            prefixIterator.close();
            iterators.remove(iterator);
            iterators.remove(prefixIterator);
        }
    }
}
