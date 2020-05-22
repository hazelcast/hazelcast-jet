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
import com.hazelcast.jet.core.JetTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyMap;

public class RocksMapTest extends JetTestSupport {
    private RocksDBStateBackend rocksDBStateBackend;
    private InternalSerializationService serializationService;
    private RocksMap<String, Integer> rocksMap;


    @Before
    public void init() {
        serializationService = getJetService(createJetMember())
                .createSerializationService(emptyMap());
        rocksDBStateBackend = new RocksDBStateBackend(serializationService);
        rocksMap = rocksDBStateBackend.getMap();
    }

    @After
    public void cleanup() {
        rocksDBStateBackend.deleteKeyValueStore();
        serializationService.dispose();
    }


    @Test
    public void test_getMap() {
        assert rocksMap != null;
    }

    @Test
    public void test_put_and_get() {
        rocksMap.put("Hello", 1);
        assert rocksMap.get("Hello") == 1;
        assert rocksMap.get("No") == null;
    }

    @Test
    public void test_putAll_and_get() {
        Map<String, Integer> map = new HashMap<>();
        map.put("hello", 1);
        map.put("bye", 2);
        rocksMap.putAll(map);
        assert rocksMap.get("bye") == 2;
        assert rocksMap.get("hello") == 1;
        assert rocksMap.get("non") == null;
    }

    @Test
    public void test_putAll_and_getAll() {
        Map<String, Integer> map = new HashMap<>();
        map.put("hello", 1);
        map.put("bye", 2);
        rocksMap.putAll(map);
        map.clear();
        map.putAll(rocksMap.getAll());
        assert map.get("bye") == 2;
        assert map.get("hello") == 1;
        assert map.get("non") == null;
    }

    //even after the column family is dropped you can still use it to get its contents
    @Test
    public void test_release_put_exception() {
        rocksMap.put("Hello", 1);
        rocksDBStateBackend.releaseMap(rocksMap);
        rocksMap.get("Hello");
        assertThrows(JetException.class, () -> rocksMap.put("bye", 2));
    }

    @Test
    public void test_iterator() {
        rocksMap.put("Hello", 1);
        rocksMap.put("bye", 2);
        rocksMap.put("Hello", 3);
        Iterator<Entry<String, Integer>> iterator = rocksMap.iterator();
        Map<String, Integer> map = new HashMap<>();
        while (iterator.hasNext()) {
            Entry<String, Integer> e = iterator.next();
            map.put(e.getKey(), e.getValue());
        }
        assert map.get("Hello") == 3;
        assert map.get("bye") == 2;
    }

    //TODO: BROKEN
    // iterator takes a snapshot of database when created, it shouldn't!
    // it should have the new values added after its creation
    @Test
    public void test_iterator_tailing() {
        rocksMap.put("Hello", 1);
        Iterator<Entry<String, Integer>> iterator = rocksMap.iterator();
        rocksMap.put("Z", 2);
        rocksMap.put("Hello", 3);
        assert iterator.hasNext();
        Entry<String, Integer> e = iterator.next();
        assert e.getKey().equals("Hello");
        assert e.getValue() == 3;
        assert iterator.hasNext();
    }
}
