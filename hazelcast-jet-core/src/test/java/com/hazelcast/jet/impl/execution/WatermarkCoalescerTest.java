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

import org.junit.Test;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static org.junit.Assert.assertEquals;

public class WatermarkCoalescerTest {

    @Test
    public void test() {
        WatermarkCoalescer wc = WatermarkCoalescer.create(20, 2);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(0));

        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
        assertEquals(10, wc.observeWm(1, 0, 10));
        assertEquals(11, wc.observeWm(1, 0, 11));
        assertEquals(Long.MIN_VALUE, wc.observeWm(1, 1, 11));
        assertEquals(Long.MIN_VALUE, wc.observeWm(1, 1, 12));
        assertEquals(12, wc.observeWm(1, 0, 12));
    }
}
