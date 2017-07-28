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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.Util;
import com.hazelcast.spi.serialization.SerializationService;
import org.junit.Test;

import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SnapshotStorageImplTest {

    private final Queue<Object> queue = new OneToOneConcurrentArrayQueue<>(1);

    @Test
    public void test_normalOperation() {
        InternalSerializationService serializer = new DefaultSerializationServiceBuilder().build();
        SnapshotStorageImpl ss = new SnapshotStorageImpl(serializer, queue);

        assertTrue(ss.offer("k1", "v"));
        assertEquals(1, queue.size());
        assertEquals(Util.entry(serializer.toData("k1"), serializer.toData("v")), queue.peek());

        assertFalse(ss.offer("k2", "v"));
        assertEquals(1, queue.size());
        assertEquals(Util.entry(serializer.toData("k1"), serializer.toData("v")), queue.peek());

        queue.clear();
        assertEquals(0, queue.size());

        assertTrue(ss.offer("k2", "v"));
        assertEquals(1, queue.size());
        assertEquals(Util.entry(serializer.toData("k2"), serializer.toData("v")), queue.peek());
    }

    @Test
    public void test_noDoubleSerialization() {
        SerializationService serializer = mock(SerializationService.class);
        SnapshotStorageImpl ss = new SnapshotStorageImpl(serializer, queue);

        assertTrue(ss.offer("k1", "v"));
        assertFalse(ss.offer("k2", "v"));
        queue.clear();
        assertTrue(ss.offer("k2", "v"));

        verify(serializer, times(1)).toData("k1");
        verify(serializer, times(1)).toData("k2");
    }
}
