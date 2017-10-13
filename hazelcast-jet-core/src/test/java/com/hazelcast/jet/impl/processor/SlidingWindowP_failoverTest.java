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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.processor.SlidingWindowP.Keys;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SlidingWindowP_failoverTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SlidingWindowP<Entry<String, Long>, LongAccumulator, Long> p;

    private void init(ProcessingGuarantee guarantee) {
        WindowDefinition wDef = WindowDefinition.tumblingWindowDef(1);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();
        p = new SlidingWindowP<>(entryKey(), Entry::getValue, wDef, aggrOp, true);

        Outbox outbox = new TestOutbox(128);
        Context context = new TestProcessorContext()
                .setProcessingGuarantee(guarantee)
                .setSnapshottingEnabled(true);
        p.init(outbox, context);
    }

    @Test
    public void when_differentWmExactlyOnce_then_fail() {
        init(EXACTLY_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT), 1L);
        exception.expect(AssertionError.class);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT), 2L);
    }

    @Test
    public void when_differentWmAtLeastOnce_then_useMin() {
        init(AT_LEAST_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT), 2L);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT), 1L);
        p.finishSnapshotRestore();

        assertEquals(1L, p.nextWinToEmit);
    }

    @Test
    public void when_noSnapshotRestored_then_wmIsMin() {
        init(AT_LEAST_ONCE);

        assertEquals(Long.MIN_VALUE, p.nextWinToEmit);
    }
}
