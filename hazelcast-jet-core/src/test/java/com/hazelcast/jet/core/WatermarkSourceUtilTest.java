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
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(0L, 2, 5, Long::longValue,
                withFixedLag(lag), suppressDuplicates());

        // all partitions are active initially
        assertNull(wsu.handleNoEvent(ns(1)));
        // now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
        assertEquals(IDLE_MESSAGE, wsu.handleNoEvent(ns(5)));
        // still all partitions are idle, but IDLE_MESSAGE should not be emitted for the second time
        assertNull(wsu.handleNoEvent(ns(5)));
        // now we observe event on partition0, watermark should be immediately forwarded because the other queue is idle
        assertEquals(new Watermark(100 - lag), wsu.observeEvent(ns(5), 100L, 0));
        // now we'll have a event on the other partition. No WM is emitted because it's older than already emitted one
        assertNull(wsu.observeEvent(ns(5), 90L, 0));
        assertEquals(new Watermark(101 - lag), wsu.observeEvent(ns(5), 101L, 0));

    }

    private long ns(long ms) {
        return MILLISECONDS.toNanos(ms);
    }
}
