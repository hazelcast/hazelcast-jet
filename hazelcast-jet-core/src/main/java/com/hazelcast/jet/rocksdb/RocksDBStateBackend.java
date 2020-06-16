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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Responsible for managing one RocksDB instance, opening, closing and deleting the database.
 * Processors use this class to acquire any number of RocksMaps they require using getMap()
 * Each RocksMap is associated with only one ColumnFamily.
 * There is only one instance of this class associated with each job.
 * The lifecycle for this class:
 * (1) ExecutionContext retrieves an instance of this class from RocksDBRegistry.
 * (2) ExecutionContext invokes initialize() with the directory and serialization service used for this job.
 * (3) Processors acquire the initialized instance from Processor.Context.rocksDBStateBackend() which calls
 * open() to open a new RocksDB instance if it wasn't already created.
 * (4) After job execution is completed, ExecutionContext invokes close() to free the RocksDB instance.
 */

public final class RocksDBStateBackend {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ArrayList<PrefixRocksMap> prefixMaps = new ArrayList<>();
    private final ArrayList<RocksMap> maps = new ArrayList<>();
    private final Options options = new RocksDBOptions().options();
    private volatile RocksDB db;
    private InternalSerializationService serializationService;
    private Path directory;


    /**
     * Initialize the State Backend with job-level information.
     *
     * @param service   the serialization service configured for this job.
     * @param directory the directory where the associated RocksDB instance will operate.
     **/
    public RocksDBStateBackend initialize(InternalSerializationService service, Path directory) throws JetException {
        this.serializationService = service;
        this.directory = directory;
        return this;
    }

    /**
     * Initialize the state backend with job-level information.
     * This method is only used for testing.
     *
     * @param serializationService the serialization serivice configured for this job.
     */
    public RocksDBStateBackend initialize(InternalSerializationService serializationService) throws JetException {
        this.serializationService = serializationService;
        try {
            String testPath = "/home/mmandouh/data";
            this.directory = Files.createTempDirectory(Path.of(testPath), "rocksdb-temp");
        } catch (IOException e) {
            throw new JetException("Failed to create RocksDB directory", e);
        }
        return this;
    }

    /**
     * Creates the associated RocksDB instance after the state backend is initialized.
     */
    public RocksDBStateBackend open() {
        if (db == null) {
            synchronized (this) {
                if (db == null) {
                    try {
                        RocksDB.loadLibrary();
                        db = RocksDB.open(options, directory.toString());
                    } catch (Exception e) {
                        throw new JetException("Failed to create a RocksDB instance", e);
                    }
                }
            }
        }
        return this;
    }

    /**
     * Returns a new RocksMap instance
     *
     * @return a new empty RocksMap
     * @throws JetException if the database is closed
     */
    @Nonnull
    public <K, V> RocksMap<K, V> getMap() throws JetException {
        assert db != null : "state backend was not opened";
        RocksMap<K, V> map = new RocksMap<>(db, getNextName(), new RocksDBOptions(), serializationService);
        maps.add(map);
        return map;
    }

    /**
     * Returns a new PrefixRocksMap instance
     *
     * @return a new empty PrefixRocksMap
     * @throws JetException if the database is closed
     */
    @Nonnull
    public <K, V> PrefixRocksMap<K, V> getPrefixMap() throws JetException {
        assert db != null : "state backend was not opened";
        PrefixRocksMap<K, V> map = new PrefixRocksMap<>(db, getNextName(), new RocksDBOptions(), serializationService);
        prefixMaps.add(map);
        return map;
    }


    /**
     * Deletes the associated RocksDB instance.
     * Should be invoked when the job finishes execution (whether successfully or with an error)
     *
     * @throws JetException if the database is closed
     */
    public void close() throws JetException {
        options.close();
        if (db != null) {
            prefixMaps.forEach(PrefixRocksMap::close);
            maps.forEach(RocksMap::close);
            db.close();
        }
    }

    @Nonnull
    private String getNextName() {
        return "RocksMap" + counter.getAndIncrement();
    }
}
