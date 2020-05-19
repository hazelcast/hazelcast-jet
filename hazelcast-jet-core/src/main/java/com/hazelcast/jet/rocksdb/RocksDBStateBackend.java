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
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Responsible for managing one RocksDB instance,
 * opening and Closing the connection and deleting the database.
 * Processors acquire an instance of this class on initialization.
 * Processors use this class to acquire any number of RocksMaps they require.
 * The database is logically partitioned using column families.
 * Each RocksMap is instantiated with a ColumnFamilyHandler.
 * Once the task has finished (complete() is invoked),
 * the task asks it to delete the whole database.
 */

public class RocksDBStateBackend {
    private final Options options;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final String directory;
    private final Serializer<String> serializer = new Serializer<>();
    private RocksDB db;
    private ArrayList<ColumnFamilyHandle> cfhs = new ArrayList<>();
    private AtomicInteger counter = new AtomicInteger(0);

    RocksDBStateBackend(@Nonnull RocksDBOptions rocksDBOptions, String directory) throws JetException {
        this.options = rocksDBOptions.getOptions();
        this.readOptions = rocksDBOptions.getReadOptions();
        this.writeOptions = rocksDBOptions.getWriteOptions();
        this.directory = directory;
        init();
    }

    private void init() throws JetException {
        RocksDB.loadLibrary();
        try {
            db = RocksDB.open(options, directory);
        } catch (RocksDBException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    public RocksMap getMap() throws JetException {
        ColumnFamilyHandle cfh;
        try {

            cfh = db.createColumnFamily(new ColumnFamilyDescriptor(serializer.serialize(getNextName())));
            cfhs.add(cfh);
            return new RocksMap(db, cfh, readOptions, writeOptions);
        } catch (RocksDBException e) {
            throw new JetException(e.getMessage(), e.getCause());
        }
    }

    // since the database is shared among all processors of a job on the same cluster member,
    // we may end up with a race condition when two processor are asking for a RocksMap at the same time
    @Nonnull
    private String getNextName() {
        return "RocksMap".concat(String.valueOf(counter.getAndIncrement()));
    }

    public void deleteKeyValueStore() {
        for (final ColumnFamilyHandle cfh : cfhs) {
            cfh.close();
        }
        db.close();
    }
}
