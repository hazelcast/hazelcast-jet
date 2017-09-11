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

package com.hazelcast.jet;

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.test.TestProcessorMetaSupplierContext;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.TestUtil.throttle;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.jet.processor.Processors.aggregateToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.insertWatermarks;
import static com.hazelcast.jet.test.TestSupport.testProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JobRestartWithSnapshotTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    private static final ConcurrentMap<List<Long>, Long> result = new ConcurrentHashMap<>();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;
    private JetTestInstanceFactory factory;

    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        JetInstance[] instances = factory.newMembers(config, 2);
        instance1 = instances[0];
        instance2 = instances[1];
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot() throws InterruptedException {
        /* Design of this test:

        It uses random partitioned generator of source events. The events are Map.Entry(partitionId, timestamp).
        For each partition timestamps from 0..elementsInPartition are generated.

        We start the test with two nodes and localParallelism(1) for source. Source instances generate items at
        the same rate of 10 per second: this causes one instance to be twice as fast as the other in terms of
        timestamp. The source processor saves partition offsets similarly to how streamKafka() and streamMap()
        do.

        After some time we shut down one instance. The job restarts from snapshot and all partitions are restored
        to single source processor instance. Partition offsets are very different, so the source is written in a way
        that it emits from the most-behind partition in order to not emit late events from more ahead partitions.

        Local parallelism of InsertWatermarkP is also 1 to avoid the edge case when different instances of
        InsertWatermarkP might initialize with first event in different frame and make them start the no-gap
        emission from different WM, which might cause the SlidingWindowP downstream to miss some of the
        first windows.

        The sink is writing to a ConcurrentMap which is an idempotent sink. It checks that on overwrite the value
        is the same. This can later be replaced with an IMap and kafka sink, after they are implemented. (IMap
        sink currently fails the job when a member is brought down.)

        The resulting contents of the sink map are compared to expected value.
         */

        DAG dag = new DAG();

        WindowDefinition wDef = WindowDefinition.tumblingWindowDef(3);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();

        result.clear();

        SequencesInPartitionsMetaSupplier sup = new SequencesInPartitionsMetaSupplier(3, 40);
        Vertex generator = dag.newVertex("generator", throttle(sup, 10))
                              .localParallelism(1);
        Vertex insWm = dag.newVertex("insWm", insertWatermarks(entry -> ((Entry<Integer, Integer>) entry).getValue(),
                withFixedLag(0), emitByFrame(wDef)))
                          .localParallelism(1);
        Vertex aggregate = dag.newVertex("aggregate", aggregateToSlidingWindow(
                t -> ((Entry<Integer, Integer>) t).getKey(),
                t -> ((Entry<Integer, Integer>) t).getValue(),
                TimestampKind.EVENT, wDef, aggrOp));

        Vertex writeMap = dag.newVertex("writeMap", AddToMapP::new)
                             .localParallelism(1);

        dag.edge(between(generator, insWm))
           .edge(between(insWm, aggregate)
                   .distributed()
                   .partitioned(entryKey()))
           .edge(between(aggregate, writeMap));

        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(1200);
        Job job = instance1.newJob(dag, config);

        Thread.sleep(3000);
        instance2.shutdown();

        job.join();

        // compute expected result
        Map<List<Long>, Long> expectedMap = new HashMap<>();
        for (long partition = 0; partition < sup.numPartitions; partition++) {
            long cnt = 0;
            for (long value = 1; value <= sup.elementsInPartition; value++) {
                cnt++;
                if (value % wDef.frameLength() == 0) {
                    expectedMap.put(asList(value, partition), cnt);
                    cnt = 0;
                }
            }
            if (cnt > 0) {
                expectedMap.put(asList(wDef.higherFrameTs(sup.elementsInPartition - 1), partition), cnt);
            }
        }

        // check expected result
        if (!expectedMap.equals(result)) {
            System.out.println("Non-received expected items: " + expectedMap.keySet().stream()
                                                                            .filter(key -> !result.containsKey(key))
                                                                            .map(Object::toString)
                                                                            .collect(joining(", ")));
            System.out.println("Received non-expected items: " + result.entrySet().stream()
                                                                       .filter(entry -> !expectedMap.containsKey(entry.getKey()))
                                                                       .map(Object::toString)
                                                                       .collect(joining(", ")));
            System.out.println("Different keys: ");
            for (Entry<List<Long>, Long> rEntry : result.entrySet()) {
                Long expectedValue = expectedMap.get(rEntry.getKey());
                if (expectedValue != null && !expectedValue.equals(rEntry.getValue())) {
                    System.out.println("key: " + rEntry.getKey() + ", expected value: " + expectedValue + ", actual value: " + rEntry.getValue());
                }
            }
            System.out.println("-- end of different keys");
            assertEquals(expectedMap, result);
        }
    }

    // This is a "test of a test" - it checks, that SequencesInPartitionsGeneratorP generates correct output
    @Test
    @Ignore
    public void test_SequencesInPartitionsGeneratorP() throws Exception {
        SequencesInPartitionsMetaSupplier pms = new SequencesInPartitionsMetaSupplier(3, 2);
        pms.init(new TestProcessorMetaSupplierContext().setLocalParallelism(1).setTotalParallelism(2));
        Address a1 = new Address("127.0.0.1", 0);
        Address a2 = new Address("127.0.0.2", 0);
        Function<Address, ProcessorSupplier> supplierFunction = pms.get(asList(a1, a2));
        Iterator<? extends Processor> processors1 = supplierFunction.apply(a1).get(1).iterator();
        Processor p1 = processors1.next();
        assertFalse(processors1.hasNext());
        Iterator<? extends Processor> processors2 = supplierFunction.apply(a2).get(1).iterator();
        Processor p2 = processors2.next();
        assertFalse(processors2.hasNext());

        testProcessor(p1, emptyList(), asList(
                entry(0, 0),
                entry(2, 0),
                entry(0, 1),
                entry(2, 1)
        ));

        testProcessor(p2, emptyList(), asList(
                entry(1, 0),
                entry(1, 1)
        ));
    }

    /**
     * A source, that will generate integer sequences from 0..ELEMENTS_IN_PARTITION,
     * one sequence for each partition.
     * <p>
     * Generated items are {@code entry(partitionId, value)}.
     */
    static class SequencesInPartitionsGeneratorP extends AbstractProcessor {

        private final int[] assignedPtions;
        private final int[] ptionOffsets;
        private final int elementsInPartition;

        private int ptionCursor;
        private MyTraverser traverser;
        private Traverser<Entry<Integer, Integer>> snapshotTraverser;

        SequencesInPartitionsGeneratorP(int[] assignedPtions, int elementsInPartition) {
            System.out.println("assignedPtions=" + Arrays.toString(assignedPtions));
            this.assignedPtions = assignedPtions;
            this.ptionOffsets = new int[assignedPtions.length];
            this.elementsInPartition = elementsInPartition;

            this.traverser = new MyTraverser();
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser, traverser::commit);
        }

        @Override
        public boolean saveSnapshot() {
            if (snapshotTraverser == null) {
                snapshotTraverser = Traversers.traverseStream(IntStream.range(0, assignedPtions.length).boxed())
                                              // save {partitionId; partitionOffset} tuples
                                              .map(i -> entry(assignedPtions[i], ptionOffsets[i]))
                                              .onFirstNull(() -> snapshotTraverser = null);
                getLogger().info("Saving snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                        + Arrays.toString(assignedPtions));
            }
            return emitSnapshotFromTraverser(snapshotTraverser);
        }

        @Override
        public void restoreSnapshot(@Nonnull Inbox inbox) {
            for (Object o; (o = inbox.poll()) != null; ) {
                Entry<Integer, Integer> e = (Entry<Integer, Integer>) o;
                int partitionIndex = arrayIndexOf(e.getKey(), assignedPtions);
                // restore offset, if assigned to us. Ignore it otherwise
                if (partitionIndex >= 0) {
                    ptionOffsets[partitionIndex] = e.getValue();
                    getLogger().info("Restored offset for partition " + e);
                }
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            // we'll start at the most-behind partition
            advanceCursor();
            return true;
        }

        private void advanceCursor() {
            ptionCursor = 0;
            int min = ptionOffsets[0];
            for (int i = 1; i < ptionOffsets.length; i++) {
                if (ptionOffsets[i] < min) {
                    min = ptionOffsets[i];
                    ptionCursor = i;
                }
            }
        }

        private class MyTraverser implements Traverser<Entry<Integer, Integer>> {
            @Override
            public Entry<Integer, Integer> next() {
                return ptionOffsets[ptionCursor] < elementsInPartition
                        ? entry(assignedPtions[ptionCursor], ptionOffsets[ptionCursor]) : null;
            }

            void commit(Entry<Integer, Integer> item) {
                assert item.getKey().equals(assignedPtions[ptionCursor]);
                assert item.getValue().equals(ptionOffsets[ptionCursor]);
                ptionOffsets[ptionCursor]++;
                advanceCursor();
            }
        }
    }

    static class SequencesInPartitionsMetaSupplier implements ProcessorMetaSupplier {

        private final int numPartitions;
        private final int elementsInPartition;

        private int globalParallelism;
        private int localParallelism;

        SequencesInPartitionsMetaSupplier(int numPartitions, int elementsInPartition) {
            this.numPartitions = numPartitions;
            this.elementsInPartition = elementsInPartition;
        }

        @Override
        public void init(@Nonnull Context context) {
            globalParallelism = context.totalParallelism();
            localParallelism = context.localParallelism();
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> {
                int startIndex = addresses.indexOf(address) * localParallelism;
                return count -> IntStream.range(0, count)
                                         .mapToObj(index -> new SequencesInPartitionsGeneratorP(
                                                 assignedPtions(startIndex + index, globalParallelism, numPartitions),
                                                 elementsInPartition))
                                         .collect(toList());
            };
        }

        private int[] assignedPtions(int processorIndex, int totalProcessors, int numPartitions) {
            return IntStream.range(0, numPartitions)
                            .filter(i -> i % totalProcessors == processorIndex)
                            .toArray();
        }

        @Nonnull
        @Override
        public SnapshotRestorePolicy snapshotRestorePolicy() {
            return SnapshotRestorePolicy.BROADCAST;
        }
    }

    private static class AddToMapP extends AbstractProcessor implements Serializable {
        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            TimestampedEntry<Integer, Long> e = (TimestampedEntry) item;
            List<Long> key = asList(e.getTimestamp(), (long) e.getKey());
            Long oldValue = result.put(key, e.getValue());
            // We are an idempotent sink, so writing the same item duplicately doesn't affect the result.
            // Let's check that the value for the key is the same as before.
            if (oldValue != null) {
                assertEquals(oldValue, e.getValue());
            }
            return true;
        }
    }
}
