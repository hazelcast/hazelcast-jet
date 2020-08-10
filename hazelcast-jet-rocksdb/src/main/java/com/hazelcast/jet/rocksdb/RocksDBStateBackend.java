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

import javax.annotation.Nonnull;
import java.util.ArrayList;


/**
 * An implementation of {@link AbstractRocksDBStateBackend} that is optimized for the general access pattern consisting
 * of interfering reads and updates.
 */
public class RocksDBStateBackend extends AbstractRocksDBStateBackend {

    private final ArrayList<RocksMap> maps;

    /**
     * Creates a new RocksDBStateBackend instance.
     */
    public RocksDBStateBackend() {
        maps = new ArrayList<>();
        rocksDBOptions = new RocksDBOptions();
        directoryName = "/rocksdb-temp";
        mapName = "RocksMap";
    }

    /**
     * Sets user defined RocksDB options for {@link RocksMap}.
     */
    public void setRocksDBOptions(RocksDBOptions rocksDBOptions) {
        this.rocksDBOptions = rocksDBOptions;
    }

    /**
     * Returns a new {@link RocksMap} instance
     *
     * @throws HazelcastException if the database is closed
     */
    @Nonnull
    public <K, V> RocksMap<K, V> getMap() throws HazelcastException {
        assert db != null : "state backend was not opened";
        RocksMap<K, V> map = new RocksMap<>(db, getNextName(),
                new RocksDBOptions(rocksDBOptions), serializationService);
        maps.add(map);
        return map;
    }

    @Override
    public void close() throws HazelcastException {
        super.close();
        for (RocksMap rocksMap : maps) {
            rocksMap.close();
        }
    }
}
