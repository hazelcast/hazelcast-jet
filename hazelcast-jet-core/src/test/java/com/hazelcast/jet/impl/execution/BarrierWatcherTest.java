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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BarrierWatcherTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private BarrierWatcher bw = new BarrierWatcher(2);

    @Test
    public void test_barriersOnAllQueues() {
        doAndCheck(0, 1, false, true, false);
        doAndCheck(1, 1, true, false, false);
        doAndCheck(0, 2, false, true, false);
        doAndCheck(1, 2, true, false, false);
        doAndCheck(0, -1, false, true, false);
        doAndCheck(1, -1, true, false, false);
    }

    @Test
    public void when_duplicateBarrierBeforeReceivingFromAll_then_error() {
        bw.observe(0, 1);
        exception.expect(AssertionError.class);
        bw.observe(0, 1);
    }

    @Test
    public void when_zeroBarrier_then_error() {
        exception.expect(AssertionError.class);
        bw.observe(0, 0);
    }

    @Test
    public void when_differentBarrierOnQueues_then_error() {
        bw.observe(0, 1);
        exception.expect(AssertionError.class);
        bw.observe(1, 2);
    }

    @Test
    public void test_markQueueDoneWithBarriersFromAll() {
        doAndCheck(0, 1, false, true, false);
        doAndCheck(1, 1, true, false, false);
        assertEquals(0, bw.markQueueDone(0));
        doAndCheck(1, 2, true, false, false);
    }

    @Test
    public void test_markQueueDoneWithNoBarrier() {
        assertEquals(0, bw.markQueueDone(0));
        assertFalse(bw.isBlocked(0));
        assertFalse(bw.isBlocked(1));
        assertEquals(0, bw.markQueueDone(1));
        assertFalse(bw.isBlocked(0));
        assertFalse(bw.isBlocked(1));
    }

    @Test
    public void test_markQueueDoneWithSomeBarriers() {
        doAndCheck(1, 1, false, false, true);
        assertEquals(1, bw.markQueueDone(0));
        doAndCheck(1, 2, true, false, false);
    }

    private void doAndCheck(int queueIndex, long snapshotId, boolean canForward,
                            boolean q0Blocked, boolean q1Blocked) {
        assertEquals("canForward does not match", canForward, bw.observe(queueIndex, snapshotId));
        assertEquals("q0Blocked does not match", q0Blocked, bw.isBlocked(0));
        assertEquals("q1Blocked does not match", q1Blocked, bw.isBlocked(1));
    }

}
