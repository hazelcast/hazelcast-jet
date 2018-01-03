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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class WatermarkCoalescerTest {

    private WatermarkCoalescer wc = WatermarkCoalescer.create(20, 2);

    @Test
    public void when_nothingHappened_then_noWm() {
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(0));
    }

    @Test
    public void when_i1RecoversFromIdle_then_wmFromI1Coalesced() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(11, wc.observeWm(0, 1, 11)); // forwarded immediately
        // When
        wc.observeEvent(0);
        // Then
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 1, 12)); // not forwarded, waiting for i1
        assertEquals(12, wc.observeWm(0, 0, 13)); // forwarded, both are at least at 12
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(13, wc.checkWmHistory(MILLISECONDS.toNanos(20))); // forwarded 13 from i1 after a delay
    }

    @Test
    public void when_i1Idle_i2HasWm_then_forwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(100, wc.observeWm(0, 1, 100));
    }

    @Test
    public void when_i1HasWm_i2Idle_then_forwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_i1_active_i2_active_then_wmForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.observeWm(0, 1, 101));
        assertEquals(101, wc.observeWm(0, 0, 101));
    }

    @Test
    public void when_i1_active_i2_activeNoWm_then_wmForwardedAfterDelay() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(100, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_activeNoWm_i2_active_then_wmForwardedAfterDelay() {
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(100, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_active_i2_idle_then_wmForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        assertEquals(100, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
    }

    @Test
    public void when_i1_idle_i2_active_then_wmForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(100, wc.observeWm(0, 1, 100));
    }

    @Test
    public void when_i1_activeNoWm_i2_activeNoWm_then_wmForwardedAfterDelay() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, 100));
        wc.observeEvent(0);
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(100, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_activeNoWm_i2_idle_then_noWmToForward() {
        wc.observeEvent(0);
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_idle_i2_activeNoWm_then_wmForwardedAfterADelay() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        wc.observeEvent(1);
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(19)));
        assertEquals(Long.MIN_VALUE, wc.checkWmHistory(MILLISECONDS.toNanos(20)));
    }

    @Test
    public void when_i1_idle_i2_idle_then_idleMessageForwardedImmediately() {
        assertEquals(Long.MIN_VALUE, wc.observeWm(0, 0, IDLE_MESSAGE.timestamp()));
        assertEquals(IDLE_MESSAGE.timestamp(), wc.observeWm(0, 1, IDLE_MESSAGE.timestamp()));
    }
}
