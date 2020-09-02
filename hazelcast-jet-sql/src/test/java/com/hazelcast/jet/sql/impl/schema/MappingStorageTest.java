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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.sql.impl.SqlTestSupport.nodeEngine;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class MappingStorageTest extends SimpleTestInClusterSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory();

    private static HazelcastInstance instance;

    private MappingStorage storage;

    @BeforeClass
    public static void beforeClass() {
        instance = FACTORY.newHazelcastInstance();
    }

    @Before
    public void before() {
        storage = new MappingStorage(nodeEngine(instance));
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void when_put_then_isPresentInValues() {
        String name = "name-1";

        storage.put(name, mapping(name, "type"));

        assertThat(storage.values().filter(m -> m.name().equals(name))).isNotEmpty();
    }

    @Test
    public void when_putIfAbsent_then_doesNotOverride() {
        String name = "name-2";

        assertThat(storage.putIfAbsent(name, mapping(name, "type-1"))).isTrue();
        assertThat(storage.putIfAbsent(name, mapping(name, "type-2"))).isFalse();

        assertThat(storage.values().filter(m -> m.type().equals("type-1"))).isNotEmpty();
        assertThat(storage.values().filter(m -> m.type().equals("type-2"))).isEmpty();
    }

    @Test
    public void when_remove_then_isNotPresentInValues() {
        String name = "name-3";

        storage.put(name, mapping(name, "type"));

        assertThat(storage.remove(name)).isTrue();

        assertThat(storage.values().filter(m -> m.name().equals(name))).isEmpty();
    }

    @Test
    public void when_removeAbsentValue_then_returnsFalse() {
        assertThat(storage.remove("name-4")).isFalse();
    }

    private static Mapping mapping(String name, String type) {
        return new Mapping(name, type, emptyList(), emptyMap());
    }
}
