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


import org.junit.Before;
import org.junit.Test;

import java.util.stream.LongStream;

public class LongStreamCastingTest extends AbstractStreamTest {

    private LongStream stream;

    @Before
    public void setUp() {
        IStreamList<Integer> list = getList();
        stream = list.stream().mapToLong(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testMap() {
        stream.map(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testFlatMap() {
        stream.flatMap(LongStream::of);
    }

    @Test(expected = ClassCastException.class)
    public void testCollect() {
        stream.collect(() -> new Long[]{0L},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test(expected = ClassCastException.class)
    public void testForEach() {
        stream.forEach(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void testForEachOrdered() {
        stream.forEachOrdered(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void testAllMatch() {
        stream.allMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testAnyMatch() {
        stream.anyMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testNoneMatch() {
        stream.noneMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testFilter() {
        stream.filter(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToObj() {
        stream.mapToObj(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToInt() {
        stream.mapToInt(m -> (int) m);
    }

    @Test(expected = ClassCastException.class)
    public void testMapToDouble() {
        stream.mapToDouble(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void testPeek() {
        stream.peek(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void testReduce() {
        stream.reduce((l, r) -> l + r);
    }

    @Test(expected = ClassCastException.class)
    public void testReduce2() {
        stream.reduce(0, (l, r) -> l + r);
    }
}
