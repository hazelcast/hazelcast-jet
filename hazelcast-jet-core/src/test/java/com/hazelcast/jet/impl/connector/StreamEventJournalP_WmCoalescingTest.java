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
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class StreamEventJournalP_WmCoalescingTest extends JetTestSupport {

    private static final int JOURNAL_CAPACITY = 10;

    private MapProxyImpl<Integer, Integer> map;
    private int[] partitionKeys;

    @Before
    public void setUp() {
        JetConfig config = new JetConfig();

        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName("*")
                .setCapacity(JOURNAL_CAPACITY)
                .setEnabled(true);

        config.getHazelcastConfig().setProperty(PARTITION_COUNT.getName(), "2");
        config.getHazelcastConfig().addEventJournalConfig(journalConfig);
        JetInstance instance = this.createJetMember(config);

        assert map == null;
        map = (MapProxyImpl<Integer, Integer>) instance.getHazelcastInstance().<Integer, Integer>getMap("test");

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

        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 2000))
                   .disableProgressAssertion()
                   .disableRunUntilCompleted(1000)
                   .disableSnapshots()
                   .expectOutput(asList(10, wm(10), 10));
    }

    @Test
    public void when_entryInOnePartition_then_wmForwardedAfterIdleTime() {
        // initially, there will be entries in both partitions
        map.put(partitionKeys[0], 10);
        map.put(partitionKeys[1], 10);

        // insert to map in parallel to verifyProcessor so that the partition0 is not marked as idle
        // but partition1 is
        new Thread(() -> {
            for (int i = 0; i < 8; i++) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(500));
                map.put(partitionKeys[0], 10);
            }
        }).start();

        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 2000))
                   .disableProgressAssertion()
                   .disableRunUntilCompleted(4000)
                   .disableSnapshots()
                   .outputChecker((e, a) -> new HashSet<>(e).equals(new HashSet<>(a)))
                   .expectOutput(asList(10, wm(10)));
    }

    @Test
    public void when_allPartitionsIdle_then_idleMessageOutput() {
        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 500))
                   .disableProgressAssertion()
                   .disableRunUntilCompleted(1500)
                   .disableSnapshots()
                   .expectOutput(singletonList(IDLE_MESSAGE));
    }

    @Test
    public void when_allPartitionsIdleAndThenRecover_then_wmOutput() throws Exception {
        // Insert to map in parallel to verifyProcessor.
        Thread updatingThread = new Thread(() -> uncheckRun(() -> {
            // We will start after a delay so that the source will first become idle and then recover.
            Thread.sleep(2000);
            for (int i = 0; i < 16; i++) {
                map.put(partitionKeys[0], 10);
                Thread.sleep(250);
            }
        }));
        updatingThread.start();

        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 1000))
                   .disableProgressAssertion()
                   .disableRunUntilCompleted(3000)
                   .disableSnapshots()
                   .outputChecker((e, a) -> {
                       a.removeAll(singletonList(10));
                       return a.equals(e);
                   })
                   .expectOutput(asList(IDLE_MESSAGE, wm(10)));

        updatingThread.interrupt();
        updatingThread.join();
    }

    @Test
    public void test_nonFirstPartition() {
        /* aim of this test is to check that the mapping from partitionIndex to partitionId works */
        map.put(partitionKeys[1], 10);

        TestSupport.verifyProcessor(createSupplier(singletonList(1), 2000))
                   .disableProgressAssertion()
                   .disableRunUntilCompleted(4000)
                   .disableSnapshots()
                   .expectOutput(asList(wm(10), 10, IDLE_MESSAGE));
    }

    public Supplier<Processor> createSupplier(List<Integer> assignedPartitions, long idleTimeout) {
        return () -> new StreamEventJournalP<>(map, assignedPartitions, e -> true,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, false,
                wmGenParams(Integer::intValue, withFixedLag(0), suppressDuplicates(), idleTimeout));
    }

    private Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }
}
