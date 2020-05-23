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
import org.junit.Assert;
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

    @Before
    public void init() {
        serializationService = getJetService(createJetMember())
                .createSerializationService(emptyMap());
        rocksDBStateBackend = new RocksDBStateBackend(serializationService);
    }

    @After
    public void cleanup() {
        rocksDBStateBackend.deleteKeyValueStore();
        serializationService.dispose();
    }

    @Test
    public void test_getMap() {
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        Assert.assertNotNull("failed to create RocksMap ", rocksMap);
    }

    @Test
    public void test_put_and_get() {
        //Given
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        //When
        rocksMap.put("key1", 1);
        //Then
        Assert.assertNotNull("the insert wasn't applied", rocksMap.get("key1"));
        if (rocksMap.get("key1") != 1) {
            Assert.fail("get doesn't return the correct value for key1");
        }
    }

    //Assert.assertEquals doesn't accept nullable parameters
    //It can't be used to check on the contents of a map
    @Test
    public void test_update() {
        //Given
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        //When
        rocksMap.put("key1", 1);
        rocksMap.put("key1", 2);
        //Then
        Assert.assertNotNull("the insert wasn't applied", rocksMap.get("key1"));
        if (rocksMap.get("key1") != 2) {
            Assert.fail("get returns the old value for key1");
        }
    }

    @Test
    public void test_putAll() {
        //Given
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        Map<String, Integer> map = new HashMap<>();
        //When
        map.put("key1", 1);
        map.put("key2", 2);
        rocksMap.putAll(map);
        //Then
        Assert.assertNotNull("the write batch wasn't applied: key1", rocksMap.get("key1"));
        Assert.assertNotNull("the write batch wasn't applied : key2", rocksMap.get("key2"));
        if (rocksMap.get("key1") != 1) {
            Assert.fail("get doesn't return the correct value for key1");
        } else if (rocksMap.get("key2") != 2) {
            Assert.fail("get doesn't return the correct value for key2");
        }
    }

    @Test
    public void test_getAll() {
        //Given
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        //When
        rocksMap.put("key1", 1);
        rocksMap.put("key2", 2);
        Map<String, Integer> map = new HashMap<>(rocksMap.getAll());
        //Then
        Assert.assertNotNull("the insert wasn't applied for key1", rocksMap.get("key1"));
        Assert.assertNotNull("the insert wasn't applied for key2", rocksMap.get("key2"));
        if (map.get("key1") != 1) {
            Assert.fail("getAll doesn't return the correct value for key1");
        } else if (map.get("key2") != 2) {
            Assert.fail("getAll doesn't return the correct value for key2");
        }
    }

    //even after the column family is dropped you can still use it to get its contents
    //but you can't modify it
    @Test
    public void test_release_put_exception() {
        //Given
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        //When
        rocksMap.put("Hello", 1);
        rocksDBStateBackend.releaseMap(rocksMap);
        rocksMap.get("Hello");
        //Then
        assertThrows(JetException.class, () -> rocksMap.put("bye", 2));
    }

    @Test
    public void test_iterator() {
        //Given
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        Map<String, Integer> map = new HashMap<>();
        Entry<String, Integer> e;
        //When
        rocksMap.put("key1", 1);
        rocksMap.put("key2", 2);
        Iterator<Entry<String, Integer>> iterator = rocksMap.iterator();
        rocksMap.put("key1", 3);
        //Then
        Assert.assertTrue(iterator.hasNext());
        e = iterator.next();
        map.put(e.getKey(), e.getValue());
        Assert.assertTrue((iterator.hasNext()));
        e = iterator.next();
        map.put(e.getKey(), e.getValue());
        Assert.assertNotNull("iterator can't access the applied insert for key1", rocksMap.get("key1"));
        Assert.assertNotNull("iterator can't access the applied insert for key2", rocksMap.get("key2"));
        if (map.get("key1") == 3) {
            Assert.fail("iterator returns a value added after its creation");
        } else if (map.get("key1") != 1) {
            Assert.fail("iterator doesn't return the correct value for key1");
        } else if (map.get("key2") != 2) {
            Assert.fail("getAll doesn't return the correct value for key2");
        }
    }
}
