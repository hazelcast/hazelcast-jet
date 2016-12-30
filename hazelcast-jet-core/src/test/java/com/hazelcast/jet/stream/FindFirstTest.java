/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream;

import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FindFirstTest extends AbstractStreamTest {

    @Test
    public void testFindFirst_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        Optional<Map.Entry<String, Integer>> first = map.stream().findFirst();

        assertTrue(first.isPresent());
        Map.Entry<String, Integer> entry = first.get();

        assertTrue(map.containsKey(entry.getKey()));
        assertEquals(map.get(entry.getKey()), entry.getValue());
    }

    @Test
    public void findFirst_whenSourceEmptyMap() {
        IStreamMap<String, Integer> map = getMap();

        Optional<Map.Entry<String, Integer>> first = map.stream().findFirst();

        assertFalse(first.isPresent());
    }

    @Test
    public void testFindFirst_whenSourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        Optional<Integer> first = list.stream().findFirst();

        assertTrue(first.isPresent());
        assertEquals(0, (int) first.get());
    }

    @Test
    public void findFirst_whenSourceEmptyList() {
        IStreamList<Integer> list = getList();

        Optional<Integer> first = list.stream().findFirst();

        assertFalse(first.isPresent());
    }
}
