/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.function;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.util.function.ComparatorEx;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.function.ComparatorEx.comparing;
import static com.hazelcast.util.function.ComparatorEx.comparingDouble;
import static com.hazelcast.util.function.ComparatorEx.comparingInt;
import static com.hazelcast.util.function.ComparatorEx.comparingLong;
import static com.hazelcast.util.function.ComparatorEx.naturalOrder;
import static com.hazelcast.util.function.ComparatorEx.reverseOrder;
import static com.hazelcast.util.function.ComparatorEx.nullsFirst;
import static com.hazelcast.util.function.ComparatorEx.nullsLast;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
public class ComparatorExTest {

    @Test
    public void when_reverseComparator() {
        assertSame(reverseOrder(), naturalOrder().reversed());
        assertSame(naturalOrder(), reverseOrder().reversed());
    }

    @Test
    public void when_reverseOrderComparator() {
        ComparatorEx c = reverseOrder();
        assertEquals(1, c.compare(1, 2));
        assertEquals(-1, c.compare(2, 1));
    }

    @Test
    public void when_nullsFirstComparator() {
        ComparatorEx c = nullsFirst(naturalOrder());
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void when_nullsLastComparator() {
        ComparatorEx c = nullsLast(naturalOrder());
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void when_nullsFirst_withoutWrapped() {
        ComparatorEx c = nullsFirst(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void when_nullsLast_withoutWrapped() {
        ComparatorEx c = nullsLast(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void testSerializable_naturalOrder() {
        checkSerializable(naturalOrder(), null);
    }

    @Test
    public void testSerializable_reverseOrder() {
        checkSerializable(reverseOrder(), null);
    }

    @Test
    public void testSerializable_thenComparing_keyExtractor() {
        checkSerializable(
                naturalOrder()
                                     .thenComparing(Object::toString),
                null);
    }

    @Test
    public void testSerializable_thenComparing_otherComparator() {
        checkSerializable(
                naturalOrder()
                                     .thenComparing(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_thenComparing_keyExtractor_keyComparator() {
        checkSerializable(
                naturalOrder()
                                     .thenComparing(Object::toString, Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_thenComparingInt() {
        checkSerializable(
                naturalOrder()
                                     .thenComparingInt(Object::hashCode),
                null);
    }

    @Test
    public void testSerializable_thenComparingLong() {
        checkSerializable(
                ComparatorEx.<Long>naturalOrder()
                                      .thenComparingLong(Long::longValue),
                null);
    }

    @Test
    public void testSerializable_thenComparingDouble() {
        checkSerializable(
                ComparatorEx.<Double>naturalOrder()
                        .thenComparingDouble(Double::doubleValue),
                null);
    }

    @Test
    public void testSerializable_nullsFirst() {
        checkSerializable(
                ComparatorEx.<Comparable>nullsFirst(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_nullsLast() {
        checkSerializable(
                ComparatorEx.<Comparable>nullsLast(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_comparing_keyExtractor() {
        checkSerializable(comparing(Object::toString), null);
    }

    @Test
    public void testSerializable_comparing_keyExtractor_keyComparator() {
        checkSerializable(comparing(Object::toString, String::compareTo), null);
    }

    @Test
    public void testSerializable_comparingInt() {
        checkSerializable(comparingInt(Object::hashCode), null);
    }

    @Test
    public void testSerializable_comparingLong() {
        checkSerializable(comparingLong(Long::longValue), null);
    }

    @Test
    public void testSerializable_comparingDouble() {
        checkSerializable(comparingDouble(Double::doubleValue), null);
    }

}
