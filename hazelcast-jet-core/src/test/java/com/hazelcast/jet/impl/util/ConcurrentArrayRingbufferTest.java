/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.impl.util.ConcurrentArrayRingbuffer.RingbufferCopy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConcurrentArrayRingbufferTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConcurrentArrayRingbuffer<Integer> rb = new ConcurrentArrayRingbuffer<>(3);

    @Test
    public void test() {
        assertEquals(0, rb.size());
        assertTrue(rb.isEmpty());
        assertArrayEquals(new Integer[]{}, rb.copyFrom(0).elements());
        rb.add(1);
        assertEquals(1, rb.size());
        assertFalse(rb.isEmpty());
        assertArrayEquals(new Integer[]{1}, rb.copyFrom(0).elements());
        rb.add(2);
        assertEquals(2, rb.size());
        assertArrayEquals(new Integer[]{1, 2}, rb.copyFrom(0).elements());
        rb.add(3);
        assertEquals(3, rb.size());
        assertArrayEquals(new Integer[]{1, 2, 3}, rb.copyFrom(0).elements());
        rb.add(4);
        assertEquals(3, rb.size());
        for (int i = 5; i < 8; i++) {
            assertArrayEquals(IntStream.range(i - rb.getCapacity(), i).boxed().toArray(Integer[]::new),
                    rb.copyFrom(0).elements());
            rb.add(i);
            assertEquals(3, rb.size());
        }
    }

    @Test
    public void when_sequenceTooHigh_then_fail() {
        exception.expect(IllegalArgumentException.class);
        rb.get(0);
    }

    @Test
    public void when_sequenceTooLow_then_fail() {
        exception.expect(IllegalArgumentException.class);
        rb.get(-1);
    }

    @Test
    public void test_get() {
        rb.add(1);
        assertEquals(1, (int) rb.get(0));
        rb.add(2);
        assertEquals(1, (int) rb.get(0));
        assertEquals(2, (int) rb.get(1));
        rb.add(3);
        assertEquals(1, (int) rb.get(0));
        assertEquals(2, (int) rb.get(1));
        assertEquals(3, (int) rb.get(2));
        rb.add(4);
        assertEquals(2, (int) rb.get(1));
        assertEquals(3, (int) rb.get(2));
        assertEquals(4, (int) rb.get(3));
    }

    @Test
    public void test_clear() {
        test();
        rb.clear();
        test();
    }

    @Test
    public void test_copyFromSequence() {
        rb.add(1);
        rb.add(2);
        RingbufferCopy result = rb.copyFrom(0);
        rb.add(3);
        result = rb.copyFrom(result.tail());
        assertArrayEquals(new Integer[]{3}, result.elements());
    }
}
