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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FixedArrayDequeTest {

    @Test
    public void smokeTest() {
        FixedArrayDeque<Integer> q = new FixedArrayDeque<>(256);
        q.add(1);
        assertEquals(1, q.size());
        q.add(2);
        assertEquals(2, q.size());
        q.add(3);
        assertEquals(3, q.size());
        q.add(4);
        assertEquals(3, q.size());
        assertEquals(Integer.valueOf(2), q.poll());
        assertEquals(2, q.size());
        q.add(5);
        assertEquals(3, q.size());
        assertEquals(Integer.valueOf(3), q.poll());
        assertEquals(Integer.valueOf(4), q.poll());
        assertEquals(Integer.valueOf(5), q.poll());
        assertTrue(q.isEmpty());
    }
}
