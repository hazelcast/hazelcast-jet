/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HashRangeTest {

    @Test
    public void partition() {
        HashRange range = new HashRange(1000, 2000);
        assertEquals(new HashRange(1000, 1500), range.partition(0, 2));
        assertEquals(new HashRange(1500, 2000), range.partition(1, 2));
    }

    @Test
    public void contains() {
        HashRange range = new HashRange(1000, 2000);
        assertFalse(range.contains("999"));
        assertTrue(range.contains("1000"));
        assertTrue(range.contains("1999"));
        assertFalse(range.contains("2000"));
        assertFalse(range.contains("2001"));
    }
}
