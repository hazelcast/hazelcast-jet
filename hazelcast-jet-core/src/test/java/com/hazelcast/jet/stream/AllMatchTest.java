/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;

public class AllMatchTest extends AbstractStreamTest {

    @Test
    public void testAllMatchTrue_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .allMatch(m -> m.getValue() < COUNT);


        assertEquals(true, found);
    }

    @Test
    public void testAllMatchFalse_whenSourceMap() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .allMatch(m -> m.getValue() > COUNT / 2);


        assertEquals(false, found);
    }

    @Test
    public void testAllMatchTrue_whenIntermediateOperation() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .map(Map.Entry::getValue)
                           .allMatch(m -> m < COUNT);


        assertEquals(true, found);
    }

    @Test
    public void testAllMatchFalse_whenIntermediateOperation() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .map(Map.Entry::getValue)
                           .allMatch(m -> m > COUNT / 2);


        assertEquals(false, found);
    }

    @Test
    public void testAllMatch_whenSourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        boolean found = list.stream()
                            .allMatch(l -> l < COUNT);

        assertEquals(true, found);
    }

}
