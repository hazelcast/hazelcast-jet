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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.test.TestSupport.COMPARE_AS_SET;
import static com.hazelcast.jet.core.test.TestSupport.drainOutbox;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class StreamEventJournalP_WmCoalescingTest extends JetTestSupport {

    private static final int JOURNAL_CAPACITY = 10;

    private MapProxyImpl<Integer, Integer> map;
    private Supplier<Processor> supplierBothPartitions;
    private Supplier<Processor> supplierPartition1;
    private int[] partitionKeys;

    @Before
    public void setUp() throws Exception {
        JetConfig config = new JetConfig();

        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName("*")
                .setCapacity(JOURNAL_CAPACITY)
                .setEnabled(true);

        config.getHazelcastConfig().setProperty(PARTITION_COUNT.getName(), "2");
        config.getHazelcastConfig().addEventJournalConfig(journalConfig);
        JetInstance instance = this.createJetMember(config);

        map = (MapProxyImpl<Integer, Integer>) instance.getHazelcastInstance().<Integer, Integer>getMap("test");

        supplierBothPartitions = () -> new StreamEventJournalP<>(map, asList(0, 1), e -> true,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, false, Integer::intValue,
                withFixedLag(0), suppressDuplicates(), 2000);
        supplierPartition1 = () -> new StreamEventJournalP<>(map, singletonList(1), e -> true,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, false, Integer::intValue,
                withFixedLag(0), suppressDuplicates(), 2000);

        partitionKeys = new int[2];
        for (int i = 1; IntStream.of(partitionKeys).anyMatch(val -> val == 0); i++) {
            int partitionId = instance.getHazelcastInstance().getPartitionService().getPartition(i).getPartitionId();
            partitionKeys[partitionId] = i;
        }
    }

    @Test
    public void when_entryInEachPartition_then_wmForwarded() {
        map.put(partitionKeys[0], 10);
        map.put(partitionKeys[1], 10);

        TestSupport.verifyProcessor(supplierBothPartitions)
                   .disableProgressAssertion()
                   .disableRunUntilCompleted(1000)
                   .expectOutput(asList(10, 10, wm(10)));
    }

    @Test
    public void smokeTest() throws Exception {
        for (int i = 0; i < 4; i++) {
            map.put(i, i);
        }

        TestSupport.verifyProcessor(supplierBothPartitions)
                   .disableProgressAssertion() // no progress assertion because of async calls
                   .disableRunUntilCompleted(1000) // processor would never complete otherwise
                   .outputChecker(COMPARE_AS_SET) // ordering is only per partition
                   .expectOutput(asList(0, 1, 2, 3));
    }

    @Test
    public void when_newData() {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        Queue<Object> queue = outbox.queueWithOrdinal(0);
        List<Object> actual = new ArrayList<>();
        Processor p = supplierBothPartitions.get();

        p.init(outbox, new TestProcessorContext());

        // putting JOURNAL_CAPACITY can overflow as capacity is per map and partitions
        // can be unbalanced.
        int batchSize = JOURNAL_CAPACITY / 2 + 1;
        int i = 0;
        for (i = 0; i < batchSize; i++) {
            map.put(i, i);
        }
        // consume
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            drainOutbox(queue, actual, true);
            assertEquals("consumed more items than expected", batchSize, actual.size());
            assertEquals(IntStream.range(0, batchSize).boxed().collect(Collectors.toSet()), new HashSet<>(actual));
        });

        for (; i < batchSize * 2; i++) {
            map.put(i, i);
        }

        // consume again
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            drainOutbox(queue, actual, true);
            assertEquals("consumed more items than expected", JOURNAL_CAPACITY + 2, actual.size());
            assertEquals(IntStream.range(0, batchSize * 2).boxed().collect(Collectors.toSet()), new HashSet<>(actual));
        });
    }

    @Test
    public void when_staleSequence() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        Queue<Object> queue = outbox.queueWithOrdinal(0);
        Processor p = supplierBothPartitions.get();
        p.init(outbox, new TestProcessorContext());

        // overflow journal
        for (int i = 0; i < JOURNAL_CAPACITY * 2; i++) {
            map.put(i, i);
        }

        // fill and consume
        List<Object> actual = new ArrayList<>();
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            drainOutbox(queue, actual, true);
            assertTrue("consumed less items than expected", actual.size() >= JOURNAL_CAPACITY);
        });

        for (int i = 0; i < JOURNAL_CAPACITY; i++) {
            map.put(i, i);
        }
    }

    @Test
    public void when_staleSequence_afterRestore() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        final Processor p = supplierBothPartitions.get();
        p.init(outbox, new TestProcessorContext());
        List<Object> output = new ArrayList<>();

        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            drainOutbox(outbox.queueWithOrdinal(0), output, true);
            assertTrue("consumed more items than expected", output.size() == 0);
        });

        assertTrueEventually(() -> {
            assertTrue("Processor did not finish snapshot", p.saveToSnapshot());
        });

        // overflow journal
        for (int i = 0; i < JOURNAL_CAPACITY * 2; i++) {
            map.put(i, i);
        }

        List<Entry> snapshotItems = outbox.snapshotQueue().stream()
                  .map(e -> entry(e.getKey().getObject(), e.getValue().getObject()))
                  .collect(Collectors.toList());

        System.out.println("Restoring journal");
        // restore from snapshot
        assertRestore(snapshotItems);

    }

    private void assertRestore(List<Entry> snapshotItems) {
        Processor p = supplierBothPartitions.get();
        TestOutbox newOutbox = new TestOutbox(new int[]{16}, 16);
        List<Object> output = new ArrayList<>();
        p.init(newOutbox, new TestProcessorContext());
        TestInbox inbox = new TestInbox();

        inbox.addAll(snapshotItems);
        p.restoreFromSnapshot(inbox);
        p.finishSnapshotRestore();

        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            drainOutbox(newOutbox.queueWithOrdinal(0), output, true);
            assertEquals("consumed different number of items than expected", JOURNAL_CAPACITY, output.size());
        });
    }

    private Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }
}
