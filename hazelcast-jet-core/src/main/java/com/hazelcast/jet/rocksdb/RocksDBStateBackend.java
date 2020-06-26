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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.JetProperties.JET_HOME;


/**
 * Responsible for managing one RocksDB instance, opening, closing and deleting the database.
 * Processors use this class to acquire any number of RocksDB-backed maps they require.
 * Each map is associated with only one RocksDB ColumnFamily.
 * There is only one instance of this class associated with each job.
 * Lifecycle for this class:
 * (1) ExecutionContext associated with a job creates two instances of this class
 * one for prefix mode and the other for regular mode.
 * (2) ExecutionContext invokes initialize() with the directory and serialization service used for this job.
 * (3) Processors acquire the initialized instance from Processor.Context.rocksDBStateBackend() which calls
 * open() to create a new RocksDB instance if it wasn't already created.
 * (4) After job execution is completed, ExecutionContext invokes close() to free the RocksDB instance.
 */

public final class RocksDBStateBackend {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ArrayList<RocksMap> maps = new ArrayList<>();
    private final ArrayList<PrefixRocksMap> prefixMaps = new ArrayList<>();
    private Options options = new RocksDBOptions().options();
    private Options prefixOptions = new PrefixRocksDBOptions().options();
    private volatile RocksDB db;
    private InternalSerializationService serializationService;
    private Path directory;
    private boolean usePrefix;

    /**
     * Initialize the state backend with job-level serialization service and creates it directory.
     *
     * @param service   the serialization service configured for this job.
     * @param directory the directory where RocksDB creates its temp directory.
     */
    public RocksDBStateBackend initialize(InternalSerializationService service, Path directory) throws JetException {
        serializationService = service;
        try {
            this.directory = Files.createTempDirectory(directory, "rocksdb-temp");
        } catch (IOException e) {
            throw new JetException("Failed to create RocksDB directory", e);
        }
        return this;
    }

    /**
     * Initialize the state backend with job-level serialization service and creates it directory.
     *
     * @param service the serialization service configured for this job.
     */
    public RocksDBStateBackend initialize(InternalSerializationService service) throws JetException {
        initialize(service, defaultPath());
        return this;
    }

    private Path defaultPath() {
        String jetHome = new File(System.getProperty(JET_HOME.getName(), JET_HOME.getDefaultValue())).getAbsolutePath();
        return Path.of(jetHome + "/rocksdb");
    }

    /**
     * Sets whether the state backend will be used in prefix mode.
     * This method shouldn't be called after open().
     */
    public RocksDBStateBackend usePrefixMode(boolean enable) {
        usePrefix = enable;
        return this;
    }

    /**
     *  Returns the directory where RocksDB instance will operate
     */
    public Path directory() {
        return directory;
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
                        if (usePrefix) {
                            db = RocksDB.open(prefixOptions, directory.toString());
                        } else {
                            db = RocksDB.open(options, directory.toString());
                        }
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
     * @throws JetException if the database is closed
     */
    @Nonnull
    public <K, V> RocksMap<K, V> getMap() throws JetException {
        assert db != null : "state backend was not opened";
        assert !usePrefix : "state backend was opened in prefix mode";
        RocksMap<K, V> map = new RocksMap<>(db, getNextName(), new RocksDBOptions(), serializationService);
        maps.add(map);
        return map;
    }

    /**
     * Returns a new PrefixRocksMap instance.
     *
     * @throws JetException if the database is closed.
     */
    @Nonnull
    public <K, V> PrefixRocksMap<K, V> getPrefixMap() throws JetException {
        assert db != null : "state backend was not opened";
        assert usePrefix : "state backend wasn't opened in prefix mode";
        PrefixRocksMap<K, V> map = new PrefixRocksMap<>(db, getNextName(),
                new PrefixRocksDBOptions(), serializationService);
        prefixMaps.add(map);
        return map;
    }

    /**
     * Deletes the associated RocksDB instance.
     * Must be invoked when the job finishes execution.
     *
     * @throws JetException if the database is closed.
     */
    public void close() throws JetException {
        if (db != null) {
            for (RocksMap rocksMap : maps) {
                rocksMap.close();
            }
            for (PrefixRocksMap prefixRocksMap : prefixMaps) {
                prefixRocksMap.close();
            }
            options.close();
            prefixOptions.close();
            db.close();
        }
    }

    @Nonnull
    private String getNextName() {
        return "RocksMap" + counter.getAndIncrement();
    }
}
