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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.core.JetTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrefixRocksMapTest extends JetTestSupport {

    private static RocksDBStateBackend rocksDBStateBackend;
    private static InternalSerializationService serializationService;
    private PrefixRocksMap<String, Integer> prefixRocksMap;

    @AfterAll
    static void cleanup() {
        rocksDBStateBackend.close();
        IOUtil.delete(rocksDBStateBackend.directory());
        serializationService.dispose();
    }

    @BeforeAll
    static void init() {
        serializationService = JetTestSupport.getJetService(Jet.bootstrappedInstance())
                                             .createSerializationService(emptyMap());
        rocksDBStateBackend = new RocksDBStateBackend().initialize(serializationService)
                                                       .usePrefixMode(true).open();
    }

    @BeforeEach
    void initTest() {
        prefixRocksMap = rocksDBStateBackend.getPrefixMap();
    }

    @Test
    public void when_putKeyValues_then_getKeyReturnsAllValues() {
        //Given
        String key = "key";
        int value1 = 6;
        int value2 = 2;
        int value3 = 9;
        ArrayList<Integer> values = new ArrayList<>();

        //when
        prefixRocksMap.add(key, value1);
        prefixRocksMap.add(key, value2);
        prefixRocksMap.add(key, value3);

        //Then
        Iterator<Integer> iterator = prefixRocksMap.get(prefixRocksMap.prefixRocksIterator(), key);

        while (iterator.hasNext()) {
            values.add(iterator.next());
        }
        assertEquals(3, values.size());
        assertTrue(values.contains(value1));
        assertTrue(values.contains(value1));
        assertTrue(values.contains(value1));
    }

    @Test
    public void when_createIterator_then_allKeysTotallyOrdered() {
        //Given
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        int value = 1;

        //When
        prefixRocksMap.add(key3, value);
        prefixRocksMap.add(key1, value);
        prefixRocksMap.add(key2, value);
        Iterator<Entry<String, Iterator<Integer>>> iterator = prefixRocksMap.iterator();

        //Then
        assertEquals(key1, iterator.next().getKey());
        assertEquals(key2, iterator.next().getKey());
        assertEquals(key3, iterator.next().getKey());
    }

}
