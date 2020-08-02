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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;


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
public abstract class AbstractRocksDBStateBackend {

    protected RocksDBOptions rocksDBOptions;
    protected volatile RocksDB db;
    protected InternalSerializationService serializationService;
    protected String directoryName;
    protected String mapName;
    private final AtomicInteger counter = new AtomicInteger(0);
    private Options options;
    private File directory;

    /**
     * Initialize the state backend with job-level serialization service and creates it directory.
     *
     * @param service   the serialization service configured for this job.
     * @param directory the directory where RocksDB creates its temp directory.
     */
    public AbstractRocksDBStateBackend initialize(InternalSerializationService service, String directory,
                                          long jobId) throws HazelcastException {
        serializationService = service;
        this.directory = new File(directory + directoryName + jobId);
        return this;
    }

    /**
     * Initialize the state backend with job-level serialization service and creates it directory.
     *
     * @param service the serialization service configured for this job.
     */
    public AbstractRocksDBStateBackend initialize(InternalSerializationService service, long jobId)
            throws HazelcastException {
        initialize(service, defaultPath(), jobId);
        return this;
    }

    @Nonnull
    private String defaultPath() {
        String jetHome = new File(System.getProperty("jet.home", "")).getAbsolutePath();
        return jetHome + "/rocksdb";
    }

    /**
     * Returns the directory where RocksDB instance will operate
     */
    public File directory() {
        return directory;
    }

    /**
     * Creates the associated RocksDB instance after the state backend is initialized.
     */
    public AbstractRocksDBStateBackend open() {
        if (db == null) {
            synchronized (this) {
                if (db == null) {
                    try {
                        if (!directory.isDirectory() && !directory.mkdirs()) {
                            throw new HazelcastException("Failed to create RocksDB directory");
                        }
                        options = rocksDBOptions.options();
                        db = RocksDB.open(options, directory.toString());
                    } catch (RocksDBException e) {
                        throw new HazelcastException("Failed to create a RocksDB instance", e);
                    }
                }
            }
        }
        return this;
    }
    /**
     * Deletes the associated RocksDB instance.
     * Must be invoked when the job finishes execution.
     *
     * @throws HazelcastException if the database is closed.
     */
    public void close() throws HazelcastException {
        if (db != null) {
            options.close();
            rocksDBOptions.close();
            db.close();
        }
    }

    @Nonnull
    protected String getNextName() {
        return mapName + counter.getAndIncrement();
    }
}
