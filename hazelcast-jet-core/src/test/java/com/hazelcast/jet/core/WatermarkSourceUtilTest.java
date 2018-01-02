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

import org.junit.Test;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WatermarkSourceUtilTest {

    @Test
    public void smokeTest() {
        // TODO break this test into simpler tests
        final int lag = 3;
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(5, Long::longValue,
                withFixedLag(lag), suppressDuplicates());
        wsu.increasePartitionCount(0L, 2);

        // all partitions are active initially
        assertNull(wsu.handleNoEvent(ns(1)));
        // now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
        assertEquals(IDLE_MESSAGE, wsu.handleNoEvent(ns(5)));
        // still all partitions are idle, but IDLE_MESSAGE should not be emitted for the second time
        assertNull(wsu.handleNoEvent(ns(5)));
        // now we observe event on partition0, watermark should be immediately forwarded because the other queue is idle
        assertEquals(wm(100 - lag), wsu.handleEvent(ns(5), 0, 100L));
        // now we'll have a event on the other partition. No WM is emitted because it's older than already emitted one
        assertNull(wsu.handleEvent(ns(5), 0, 90L));
        assertEquals(wm(101 - lag), wsu.handleEvent(ns(5), 0, 101L));
    }

    @Test
    public void smokeTest_disabledTimeout() {
        final int lag = 3;
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(-1, Long::longValue, withFixedLag(lag),
                suppressDuplicates());
        wsu.increasePartitionCount(2);

        // all partitions are active initially
        assertNull(wsu.handleNoEvent());
        // let's have events only in partition0. No WM is output because we wait for the other partition indefinitely
        assertNull(wsu.handleEvent(0, 10L));
        assertNull(wsu.handleEvent(0, 11L));
        // now have some events in the other partition, wms will be output
        assertEquals(wm(10 - lag), wsu.handleEvent(1, 10L));
        assertEquals(wm(11 - lag), wsu.handleEvent(1, 11L));
        // now partition1 will get ahead of partition0 -> no WM
        assertNull(wsu.handleEvent(1, 12L));
        // another event in partition0, we'll get the wm
        assertEquals(wm(12 - lag), wsu.handleEvent(0, 13L));
    }

    @Test
    public void test_zeroPartitions() {
        final int lag = 3;
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(-1, Long::longValue,
                withFixedLag(lag), suppressDuplicates());

        // it should immediately emit the idle message, even though the idle timeout is -1
        assertEquals(IDLE_MESSAGE, wsu.handleNoEvent());
        assertNull(wsu.handleNoEvent());

        // after adding a partition and observing an event, WM should be emitted
        wsu.increasePartitionCount(1);
        assertNull(wsu.handleNoEvent()); // can't send WM here, we don't know what its value would be
        assertEquals(wm(10 - lag), wsu.handleEvent(0, 10L));
    }

    private long ns(long ms) {
        return MILLISECONDS.toNanos(ms);
    }

    public Watermark wm(long time) {
        return new Watermark(time);
    }
}
