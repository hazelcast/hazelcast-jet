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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AppendableRingBufferTraverserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private AppendableRingBufferTraverser t = new AppendableRingBufferTraverser(2);

    @Test
    public void test_isEmpty() {
        assertTrue(t.isEmpty());
        t.push("foo");
        assertFalse(t.isEmpty());
        t.next();
        assertTrue(t.isEmpty());
    }

    @Test
    public void when_nothingAdded_then_nextReturnsNull() {
        assertNull(t.next());
    }

    @Test
    public void when_oneObjectAdded_then_traversed() {
        t.push("1");
        assertEquals("1", t.next());
        assertNull(t.next());
    }

    @Test
    public void when_twoObjectsAdded_then_traversed() {
        t.push("1");
        t.push("2");
        assertEquals("1", t.next());
        assertEquals("2", t.next());
        assertNull(t.next());
    }

    @Test
    public void test_addTwoRemoveOneAddOneRemoveTwo() {
        t.push("1");
        t.push("2");
        assertEquals("1", t.next());
        t.push("3");
        assertEquals("2", t.next());
        assertEquals("3", t.next());
        assertNull(t.next());
    }

    @Test
    public void when_threeObjectsAdded_then_fail() {
        t.push("1");
        t.push("2");
        exception.expectMessage("ringbuffer full");
        t.push("3");
    }

    @Test
    public void test_oneAddedAndRemovedInCycle() {
        for (int i = 0; i < 100; i++) {
            t.push(i);
            assertEquals(i, t.next());
        }
    }

    @Test
    public void test_twoAddedAndRemovedInCycle() {
        for (int i = 0; i < 100; i += 2) {
            t.push(i);
            t.push(i + 1);
            assertEquals(i, t.next());
            assertEquals(i + 1, t.next());
        }
    }

    @Test
    public void test_overflowOneItem() {
        t.head = t.tail = Integer.MAX_VALUE;
        assertTrue(t.isEmpty());
        t.push("1");
        assertFalse(t.isEmpty());
        assertEquals("1", t.next());
        assertNull(t.next());
    }

    @Test
    public void test_overflowTwoItems() {
        t.head = t.tail = Integer.MAX_VALUE;
        assertTrue(t.isEmpty());
        t.push("1");
        t.push("2");
        assertFalse(t.isEmpty());
        assertEquals("1", t.next());
        assertFalse(t.isEmpty());
        assertEquals("2", t.next());
        assertTrue(t.isEmpty());
        assertNull(t.next());
    }

    @Test
    public void when_capacityExceededAtOverflow_then_fail() {
        t.head = t.tail = Integer.MAX_VALUE - 1;
        t.push("1");
        t.push("2");
        exception.expectMessage("ringbuffer full");
        t.push("3");
    }

    @Test
    public void test_flatMapperUsage() {
        // an instance of AppendableRingBufferTraverser is repeatedly returned
        // from a flatMap function
        Traverser tt = Traverser.over(10, 20)
                .flatMap(item -> {
                    t.push(item);
                    t.push(item + 1);
                    return t;
                });

        assertEquals(10, tt.next());
        assertEquals(11, tt.next());
        assertEquals(20, tt.next());
        assertEquals(21, tt.next());
        assertNull(tt.next());
    }
}
