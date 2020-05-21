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
import com.hazelcast.jet.core.JetTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class RocksMapTest extends JetTestSupport {
    private static RocksDBStateBackend rocksDBStateBackend;

    @Before
    public void init() {
        InternalSerializationService serializationService = getJetService(createJetMember())
                .createSerializationService(emptyMap());
        rocksDBStateBackend = new RocksDBStateBackend(serializationService);
    }

    @After
    public void cleanup() {
        rocksDBStateBackend.deleteKeyValueStore();
    }

    @Test
    public void test_getMap() {
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        assert rocksMap != null;
    }

    @Test
    public void test_put_and_get() {
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        rocksMap.put("Hello", 1);
        assert rocksMap.get("Hello") == 1;
        assert rocksMap.get("No") == null;
    }

    @Test
    public void test_putAll_and_get() {
        RocksMap<String, Integer> rocksMap = rocksDBStateBackend.getMap();
        Map<String, Integer> map = new HashMap<>();
        map.put("hello", 1);
        map.put("bye", 2);
        rocksMap.putAll(map);
        assert rocksMap.get("bye") == 2;
        assert rocksMap.get("non") == null;
    }

}
