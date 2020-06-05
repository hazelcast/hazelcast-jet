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
import com.hazelcast.nio.ObjectDataOutput;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Responsible for managing one RocksDB instance,
 * opening, closing the connection and deleting the database.
 * Processors acquire an instance of this class on initialization.
 * Processors use this class to acquire any number of RocksMaps they require.
 * The database is logically partitioned using column families.
 * Each RocksMap is instantiated with a ColumnFamilyHandler.
 * Once the processor has finished execution,
 * the processor should invoke releaseMap() on each acquired map.
 */

public final class RocksDBStateBackend {
    private static final ArrayList<ColumnFamilyHandle> COLUMN_FAMILY_HANDLES = new ArrayList<>();
    private static InternalSerializationService serializationService;
    private static RocksDB db;
    private static Path directory;
    private final RocksDBOptions rocksDBOptions = new RocksDBOptions();
    private final AtomicInteger counter = new AtomicInteger(0);

    private RocksDBStateBackend() {
        try {
            RocksDB.loadLibrary();
            db = RocksDB.open(rocksDBOptions.getOptions(), directory.toString());
        } catch (Exception e) {
            throw new JetException("Failed to create a RocksDB instance", e);
        }
    }

    /**
     * Returns the singleton state backend instance.
     * This instance is lazily created when a processor needs to acquire it.
     */
    public static RocksDBStateBackend getKeyValueStore() {
        assert serializationService != null
                : "serialization service must be initialized before creating the state backend";
        assert directory != null :
                "RocksDB directory must be initialized before creating the state backend";

        return LazyHolder.INSTANCE;
    }

    /**
     * Initializes the serialization service.
     *
     * @param serializationService the job-level serialization service.
     */

    public static void setSerializationService(InternalSerializationService serializationService) {
        if (RocksDBStateBackend.serializationService == null) {
            RocksDBStateBackend.serializationService = serializationService;
        }
    }

    /**
     * Sets the directory where the state backend will be operating
     */
    public static void setDirectory(Path directory) {
        if (RocksDBStateBackend.directory == null) {
            RocksDBStateBackend.directory = directory;
        }
    }

    /**
     * Deletes the whole database instance.
     * Should be invoked when the job finishes execution (whether successfully or with an error)
     *
     * @throws JetException if the database is closed
     */
    public static synchronized void deleteKeyValueStore() throws JetException {
        if (db != null) {
            for (final ColumnFamilyHandle cfh : COLUMN_FAMILY_HANDLES) {
                try {
                    db.dropColumnFamily(cfh);
                } catch (RocksDBException e) {
                    throw new JetException("Failed to delete column family", e);
                }
            }
            db.close();
        }
    }

    /**
     * Returns a new RocksMap instance
     *
     * @return a new empty RocksMap
     * @throws JetException if the database is closed
     */
    public <K, V> RocksMap<K, V> getMap() throws JetException {
        ColumnFamilyHandle cfh;
        try {
            cfh = db.createColumnFamily(new ColumnFamilyDescriptor((serialize(getNextName()))));
            COLUMN_FAMILY_HANDLES.add(cfh);
            return new RocksMap<>(db, cfh, rocksDBOptions.getReadOptions(),
                    rocksDBOptions.getWriteOptions(), serializationService);
        } catch (RocksDBException e) {
            throw new JetException("Failed to create RocksMap", e);
        }
    }

    /**
     * Returns a new RocksMap instance with its elements copied from the supplied Map
     *
     * @throws JetException if the database is closed
     */
    public <K, V> RocksMap<K, V> getMap(Map<K, V> map) throws JetException {
        RocksMap<K, V> rocksMap = getMap();
        rocksMap.putAll(map);
        return rocksMap;
    }


    // since the database is shared among all processors of a job on the same cluster member,
    // we may end up with a race condition when two processor are asking for a RocksMap at the same time
    @Nonnull
    private String getNextName() {
        return "RocksMap" + counter.getAndIncrement();
    }

    /**
     * Deletes the supplied RocksMap
     *
     * @param map the RocksMap to be deleted
     * @throws JetException if the database is closed
     */
    public void releaseMap(@Nonnull RocksMap map) throws JetException {
        try {
            ColumnFamilyHandle cfh = map.getColumnFamilyHandle();
            if (COLUMN_FAMILY_HANDLES.contains(cfh)) {
                db.dropColumnFamily(cfh);
                COLUMN_FAMILY_HANDLES.remove(cfh);
            }
        } catch (RocksDBException e) {
            throw new JetException(e);
        }
    }

    private static class LazyHolder {
        static final RocksDBStateBackend INSTANCE = new RocksDBStateBackend();
    }

    private <T> byte[] serialize(T item) {
        ObjectDataOutput out = serializationService.createObjectDataOutput();
        serializationService.writeObject(out, item);
        return out.toByteArray();
    }
}
