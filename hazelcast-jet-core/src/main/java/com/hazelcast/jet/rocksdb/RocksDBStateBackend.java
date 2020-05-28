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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

public class RocksDBStateBackend {
    private final RocksDBOptions rocksDBOptions = new RocksDBOptions();
    private final InternalSerializationService serializationService;
    private final ArrayList<ColumnFamilyHandle> cfhs = new ArrayList<>();
    private final Path directory;
    private RocksDB db;
    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * Returns a new state backend instance.
     * This constructor is for testing purposes only.
     *
     * @param serializationService the serialization service associated with the current job
     */
    public RocksDBStateBackend(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
        try {
            directory = Files.createTempDirectory("rocksdb-temp");
        } catch (IOException e) {
            throw new JetException("failed to create RocksDB temp dierectoy", e);
        }
        init();
    }


    private void init() throws JetException {
        try {
            RocksDB.loadLibrary();
            db = RocksDB.open(rocksDBOptions.getOptions(), directory.toString());
        } catch (Exception e) {
            throw new JetException("Failed to create a RocksDB instance", e);
        }
    }

    /**
     * Returns the directory where RocksDB will be operating.
     */
    public Path getDirectory() {
        return directory;
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
            cfhs.add(cfh);
            return new RocksMap<>(db, cfh, rocksDBOptions.getReadOptions(),
                    rocksDBOptions.getWriteOptions(), serializationService);
        } catch (RocksDBException e) {
            throw new JetException("Failed to create RocksMap", e);
        }
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
    public void releaseMap(RocksMap map) throws JetException {
        try {
            ColumnFamilyHandle cfh = map.getColumnFamilyHandle();
            db.dropColumnFamily(cfh);
            cfhs.remove(cfh);
        } catch (RocksDBException e) {
            throw new JetException(e);
        }
    }

    /**
     * Deletes the whole database instance.
     * Should be invoked when the job finishes execution (whether successfully or with an error)
     *
     * @throws JetException if the database is closed
     */
    public void deleteKeyValueStore() throws JetException {
        for (final ColumnFamilyHandle cfh : cfhs) {
            try {
                db.dropColumnFamily(cfh);
            } catch (RocksDBException e) {
                throw new JetException("Failed to Delete Column Family", e);
            }
            db.close();
        }
    }

    private <T> byte[] serialize(T item) {
        ObjectDataOutput out = serializationService.createObjectDataOutput();
        serializationService.writeObject(out, item);
        return out.toByteArray();
    }
}
