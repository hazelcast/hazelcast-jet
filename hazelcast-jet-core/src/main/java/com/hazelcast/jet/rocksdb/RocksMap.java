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

import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.internal.serialization.impl.SerializerAdapter;
import com.hazelcast.jet.impl.serialization.DelegatingSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.rocksdb.RocksDB;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.emptyMap;


/**
 * RocksMap is RocksDB-backed HashMap.
 * Responsible for providing the interface of HashMap to processors.
*/
public class RocksMap<K,V> {
    private final RocksDB db;
    private final ColumnFamilyHandle cfh;
    private final DelegatingSerializationService serializationService;
    private SerializerAdapter keySerializer;
    private SerializerAdapter valueSerializer;

    RocksMap(RocksDB db, ColumnFamilyHandle cfh,Class<K> kClass,Class<V> vClass){
        this.db= db;
        this.cfh=cfh;
        serializationService = new DelegatingSerializationService(emptyMap(),SerializationServiceV1.builder().build());
        keySerializer = serializationService.serializerFor(kClass);
        valueSerializer = serializationService.serializerFor(vClass);
    }

    public V get(K key) {
       ObjectDataOutput out = serializationService.createObjectDataOutput();
        try {
            keySerializer.write(out,key);
            byte[] keyBytes = out.toByteArray();
            byte[] valueBytes = db.get(cfh,keyBytes);
            ObjectDataInput in = serializationService.createObjectDataInput(valueBytes);
            return (V) valueSerializer.read(in);
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(K key, V value) {
        ObjectDataOutput outK = serializationService.createObjectDataOutput();
        ObjectDataOutput outV = serializationService.createObjectDataOutput();
        try {
            keySerializer.write(outK,key);
            valueSerializer.write(outV,value);
            byte[] keyBytes = outK.toByteArray();
            byte[] valueBytes = outV.toByteArray();
            db.put(cfh,keyBytes,valueBytes);
        } catch (IOException | RocksDBException e) {
            e.printStackTrace();
        }
    }

    public Map<K, V> getAll() {
        return null;
    }

    public void putAll(Map<K,V> entries) {
    }

    public void delete(K key) {
    }

    public Iterator<Map.Entry<K, V>> all() {
        return null;
    }
}
