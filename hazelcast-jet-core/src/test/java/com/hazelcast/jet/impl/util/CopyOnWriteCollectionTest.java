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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
public class CopyOnWriteCollectionTest extends JetTestSupport {

    @Test
    public void size_whenEmpty_return0() {
        int size = new CopyOnWriteCollection<>().size();
        assertEquals(0, size);
    }

    @Test
    public void size_whenNullInserted_return1() {
        CopyOnWriteCollection<Object> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        assertEquals(1, coll.size());
    }

    @Test
    public void isEmpty_whenEmpty_returnTrue() {
        boolean empty = new CopyOnWriteCollection<>().isEmpty();
        assertTrue(empty);
    }

    @Test
    public void isEmpty_whenNullInserted_returnFalse() {
        CopyOnWriteCollection<Object> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        assertFalse(coll.isEmpty());
    }

    @Test
    public void contains_whenEmpty() {
        CopyOnWriteCollection<Object> coll = new CopyOnWriteCollection<>();
        assertFalse(coll.contains(null));
    }

    @Test
    public void contains_whenNotEmpty() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        coll.add("foo");

        assertTrue(coll.contains(null));
        assertTrue(coll.contains("foo"));
        assertFalse(coll.contains("bar"));
    }

    @Test
    public void toArray_whenEmpty() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        Object[] objects = coll.toArray();
        assertEquals(0, objects.length);
    }

    @Test
    public void toArray_whenNotEmpty() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        coll.add("foo");
        Object[] arr = coll.toArray();

        assertEquals(2, arr.length);
        assertArrayContains(arr, "foo");
        assertArrayContains(arr, null);
    }

    @Test
    public void toArrayInitialized_whenNotEmpty() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        coll.add("foo");

        String[] arr = new String[2];
        arr = coll.toArray(arr);

        assertEquals(2, arr.length);
        assertArrayContains(arr, "foo");
        assertArrayContains(arr, null);
    }

    @Test
    public void toArrayInitialized_whenNotEmpty_targetArraySmaller() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        coll.add("foo");

        String[] arr = new String[0];
        arr = coll.toArray(arr);

        assertEquals(2, arr.length);
        assertArrayContains(arr, "foo");
        assertArrayContains(arr, null);
    }

    @Test
    public void toArrayInitialized_whenNotEmpty_targetArrayBigger() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add(null);
        coll.add("foo");

        String[] arr = new String[]{"one", "two", "three", "four"};
        arr = coll.toArray(arr);

        assertEquals(4, arr.length);
        assertEquals(null, arr[0]);
        assertEquals("foo", arr[1]);
        assertNull(arr[2]);
    }

    @Test
    public void removeAll() {
        try {
            new CopyOnWriteCollection<>().removeAll(Collections.emptyList());
            fail("you implemented the removeAll() method - that's awesome. please do not forget to test it too");
        } catch (UnsupportedOperationException e) {
            //empty
        }
    }

    @Test
    public void retainAll() {
        try {
            new CopyOnWriteCollection<>().retainAll(Collections.emptyList());
            fail("you implemented the retainAll() method - that's awesome. please do not forget to test it too");
        } catch (UnsupportedOperationException e) {
            //empty
        }
    }

    @Test
    public void remove() {
        CopyOnWriteCollection<Object> coll = new CopyOnWriteCollection<>();
        assertFalse(coll.remove("foo"));

        coll.add("foo");
        coll.add(null);
        coll.add("bar");

        assertTrue(coll.remove(null));

        CopyOnWriteCollection<Object>.RefreshableIterator iter = coll.iterator();
        assertIteratorReturns(iter, "foo", "bar");
        assertFalse(iter.hasNext());

        assertTrue(coll.remove("bar"));
        iter.refresh();
        assertIteratorReturns(iter, "foo");
        assertFalse(iter.hasNext());

        assertTrue(coll.remove("foo"));
        iter.refresh();
        assertFalse(iter.hasNext());

        assertFalse(coll.remove("foo"));
    }

    @Test
    public void clear() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add("foo");
        coll.add("bar");
        coll.clear();
        assertTrue(coll.isEmpty());
    }

    @Test
    public void containsAll() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        List<String> listWithItems = new ArrayList<>();
        listWithItems.add(null);
        listWithItems.add("foo");

        assertFalse(coll.containsAll(listWithItems));

        coll.add("foo");
        assertFalse(coll.containsAll(listWithItems));

        coll.add(null);
        assertTrue(coll.containsAll(listWithItems));
    }

    @Test
    public void addAll() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        List<String> listWithItems = Arrays.asList("foo", "bar", null);
        coll.addAll(listWithItems);

        assertEquals(3, coll.size());
        assertTrue(coll.contains("foo"));
        assertTrue(coll.contains("bar"));
        assertTrue(coll.contains(null));
    }

    @Test
    public void iterator_whenEmpty() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        CopyOnWriteCollection<String>.RefreshableIterator iterator = coll.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("expected NoSuchElementEx");
        } catch (NoSuchElementException e) {
            //empty
        }
    }

    @Test
    public void iterator_whenNotEmpty() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add("foo");
        coll.add("bar");
        coll.add(null);

        CopyOnWriteCollection<String>.RefreshableIterator iterator = coll.iterator();
        assertIteratorReturns(iterator, "foo", "bar", null);
        assertFalse(iterator.hasNext());

        iterator.refresh();
        assertIteratorReturns(iterator, "foo", "bar", null);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void iterator_whenNotEmpty_andModifiedBetweenBeforeRefresh() {
        CopyOnWriteCollection<String> coll = new CopyOnWriteCollection<>();
        coll.add("foo");
        coll.add("bar");
        coll.add(null);

        CopyOnWriteCollection<String>.RefreshableIterator iterator = coll.iterator();
        assertIteratorReturns(iterator, "foo", "bar", null);
        assertFalse(iterator.hasNext());

        coll.clear();
        iterator.refresh();
        assertFalse(iterator.hasNext());
    }

    private static <T> void assertIteratorReturns(Iterator<T> iterator, T...elements) {
        for (T element : elements) {
            assertTrue(iterator.hasNext());
            T fromIterator = iterator.next();
            assertEquals(element, fromIterator);
        }
    }

    private static <T> void assertArrayContains(Object[] arr, T o) {
        if (o == null) {
            assertArrayContainsNull(arr);
            return;
        }
        for (Object e : arr) {
            if (o.equals(e)) {
                return;
            }
        }
        throw new AssertionError("Array does not contain element " + o + ". Array content: "
                + Arrays.toString(arr));
    }

    private static void assertArrayContainsNull(Object[] arr) {
        for (Object e : arr) {
            if (e == null) {
                return;
            }
        }
        throw new AssertionError("Array does not contain null. Array content: " + Arrays.toString(arr));
    }
}

