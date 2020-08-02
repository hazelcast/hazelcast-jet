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
 * An implementation of AbstractRocksDBStateBackend that is optimized for sequential access pattern.
 * see {@link AbstractRocksDBStateBackend}
 */
public class PrefixRocksDBStateBackend extends AbstractRocksDBStateBackend {

    private final ArrayList<PrefixRocksMap> prefixMaps;

    /**
     * Creates a new RocksDBStateBackend instance.
     */
    public PrefixRocksDBStateBackend() {
        prefixMaps = new ArrayList<>();
        rocksDBOptions = new PrefixRocksDBOptions();
        directoryName = "/prefix-rocksdb-temp";
        mapName = "PrefixRocksMap";
    }

    /**
     * Sets user defined RocksDB options for PrefixRocksMap.
     */
    public void setRocksDBOptions(PrefixRocksDBOptions rocksDBOptions) {
        this.rocksDBOptions = rocksDBOptions;
    }

    /**
     * Returns a new PrefixRocksMap instance.
     *
     * @throws HazelcastException if the database is closed.
     */
    @Nonnull
    public <K, V> PrefixRocksMap<K, V> getPrefixMap() throws HazelcastException {
        assert db != null : "state backend was not opened";
        PrefixRocksMap<K, V> map = new PrefixRocksMap<>(db, getNextName(),
                new PrefixRocksDBOptions((PrefixRocksDBOptions) rocksDBOptions), serializationService);
        prefixMaps.add(map);
        return map;
    }

    @Override
    public void close() throws HazelcastException {
        super.close();
        for (PrefixRocksMap prefixRocksMap : prefixMaps) {
            prefixRocksMap.close();
        }
    }
}
