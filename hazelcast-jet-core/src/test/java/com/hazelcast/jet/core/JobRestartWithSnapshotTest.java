/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.DummyStatefulP;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.JobRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.test.PacketFiltersUtil.delayOperationsFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class JobRestartWithSnapshotTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;

    @Before
    public void setup() {
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        instance1 = createJetMember(config);
        instance2 = createJetMember(config);
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot_singleStage() throws Exception {
        when_nodeDown_then_jobRestartsFromSnapshot(false);
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot_twoStage() throws Exception {
        when_nodeDown_then_jobRestartsFromSnapshot(true);
    }

    private void when_nodeDown_then_jobRestartsFromSnapshot(boolean twoStage) throws Exception {
        /* Design of this test:

        It uses a random partitioned generator of source events. The events are
        Map.Entry(partitionId, timestamp). For each partition timestamps from
        0..elementsInPartition are generated.

        We start the test with two nodes and localParallelism(1) and 3 partitions
        for source. Source instances generate items at the same rate of 10 per
        second: this causes one instance to be twice as fast as the other in terms of
        timestamp. The source processor saves partition offsets similarly to how
        KafkaSources.kafka() and Sources.mapJournal() do.

        After some time we shut down one instance. The job restarts from the
        snapshot and all partitions are restored to single source processor
        instance. Partition offsets are very different, so the source is written
        in a way that it emits from the most-behind partition in order to not
        emit late events from more ahead partitions.

        Local parallelism of InsertWatermarkP is also 1 to avoid the edge case
        when different instances of InsertWatermarkP might initialize with first
        event in different frame and make them start the no-gap emission from
        different WM, which might cause the SlidingWindowP downstream to miss
        some of the first windows.

        The sink writes to an IMap which is an idempotent sink.

        The resulting contents of the sink map are compared to expected value.
         */

        DAG dag = new DAG();

        SlidingWindowPolicy wDef = SlidingWindowPolicy.tumblingWinPolicy(3);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();

        IMap<List<Long>, Long> result = instance1.getMap("result");
        result.clear();

        int numPartitions = 3;
        int elementsInPartition = 250;
        DistributedSupplier<Processor> sup = () ->
                new SequencesInPartitionsGeneratorP(numPartitions, elementsInPartition, true);
        Vertex generator = dag.newVertex("generator", throttle(sup, 30))
                              .localParallelism(1);
        Vertex insWm = dag.newVertex("insWm", insertWatermarksP(eventTimePolicy(
                o -> ((Entry<Integer, Integer>) o).getValue(), limitingLag(0), wDef.frameSize(), wDef.frameOffset(), -1)))
                          .localParallelism(1);
        Vertex map = dag.newVertex("map",
                mapP((TimestampedEntry e) -> entry(asList(e.getTimestamp(), (long) (int) e.getKey()), e.getValue())));
        Vertex writeMap = dag.newVertex("writeMap", SinkProcessors.writeMapP("result"));

        if (twoStage) {
            Vertex aggregateStage1 = dag.newVertex("aggregateStage1", Processors.accumulateByFrameP(
                    singletonList((DistributedFunction<? super Object, ?>) t -> ((Entry<Integer, Integer>) t).getKey()),
                    singletonList(t1 -> ((Entry<Integer, Integer>) t1).getValue()),
                    TimestampKind.EVENT,
                    wDef,
                    aggrOp.withIdentityFinish()
            ));
            Vertex aggregateStage2 = dag.newVertex("aggregateStage2",
                    combineToSlidingWindowP(wDef, aggrOp, TimestampedEntry::fromWindowResult));

            dag.edge(between(insWm, aggregateStage1)
                    .partitioned(entryKey()))
               .edge(between(aggregateStage1, aggregateStage2)
                       .distributed()
                       .partitioned(entryKey()))
               .edge(between(aggregateStage2, map));
        } else {
            Vertex aggregate = dag.newVertex("aggregate", Processors.aggregateToSlidingWindowP(
                    singletonList((DistributedFunction<Object, Integer>) t -> ((Entry<Integer, Integer>) t).getKey()),
                    singletonList(t1 -> ((Entry<Integer, Integer>) t1).getValue()),
                    TimestampKind.EVENT,
                    wDef,
                    aggrOp,
                    TimestampedEntry::fromWindowResult));

            dag.edge(between(insWm, aggregate)
                    .distributed()
                    .partitioned(entryKey()))
               .edge(between(aggregate, map));
        }

        dag.edge(between(generator, insWm))
           .edge(between(map, writeMap));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(1200);
        Job job = instance1.newJob(dag, config);

        JobRepository jobRepository = new JobRepository(instance1);
        int timeout = (int) (MILLISECONDS.toSeconds(config.getSnapshotIntervalMillis()) + 2);

        waitForFirstSnapshot(jobRepository, job.getId(), timeout);
        waitForNextSnapshot(jobRepository, job.getId(), timeout);
        // wait a little more to emit something, so that it will be overwritten in the sink map
        Thread.sleep(300);

        instance2.getHazelcastInstance().getLifecycleService().terminate();

        // Now the job should detect member shutdown and restart from snapshot.
        // Let's wait until the next snapshot appears.
        waitForNextSnapshot(jobRepository, job.getId(),
                (int) (MILLISECONDS.toSeconds(config.getSnapshotIntervalMillis()) + 10));
        waitForNextSnapshot(jobRepository, job.getId(), timeout);

        job.join();

        // compute expected result
        Map<List<Long>, Long> expectedMap = new HashMap<>();
        for (long partition = 0; partition < numPartitions; partition++) {
            long cnt = 0;
            for (long value = 1; value <= elementsInPartition; value++) {
                cnt++;
                if (value % wDef.frameSize() == 0) {
                    expectedMap.put(asList(value, partition), cnt);
                    cnt = 0;
                }
            }
            if (cnt > 0) {
                expectedMap.put(asList(wDef.higherFrameTs(elementsInPartition - 1), partition), cnt);
            }
        }

        // check expected result
        if (!expectedMap.equals(result)) {
            System.out.println("All expected entries: " + expectedMap.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("All actual entries: " + result.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(", ")));
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
                    System.out.println("key: " + rEntry.getKey() + ", expected value: " + expectedValue
                            + ", actual value: " + rEntry.getValue());
                }
            }
            System.out.println("-- end of different keys");
            assertEquals(expectedMap, new HashMap<>(result));
        }

        for (int i = 0; i <= 1; i++) {
            assertTrue("Snapshots map " + i + " not empty after job finished",
                    instance1.getMap(snapshotDataMapName(job.getId(), i)).isEmpty());
        }
    }

    @Test
    public void when_snapshotStartedBeforeExecution_then_firstSnapshotIsSuccessful() {
        // instance1 is always coordinator
        // delay ExecuteOperation so that snapshot is started before execution is started on the worker member
        delayOperationsFrom(hz(instance1), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.START_EXECUTION_OP)
        );

        DAG dag = new DAG();
        dag.newVertex("p", FirstSnapshotProcessor::new).localParallelism(1);

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(0);
        Job job = instance1.newJob(dag, config);
        JobRepository repository = new JobRepository(instance1);

        // the first snapshot should succeed
        assertTrueEventually(() -> {
            JobExecutionRecord record = repository.getJobExecutionRecord(job.getId());
            assertNotNull("null JobRecord", record);
            assertEquals(0, record.snapshotId());
        }, 30);
    }

    private void waitForFirstSnapshot(JobRepository jr, long jobId, int timeout) {
        long[] snapshotId = {-1};
        assertTrueEventually(() -> {
            JobExecutionRecord record = jr.getJobExecutionRecord(jobId);
            assertNotNull("null JobExecutionRecord", record);
            assertTrue("No snapshot produced",
                    record.dataMapIndex() >= 0 && record.snapshotId() >= 0);
            assertTrue("stats are 0", record.snapshotStats().numBytes() > 0);
            snapshotId[0] = record.snapshotId();
        }, timeout);
        logger.info("First snapshot found (id=" + snapshotId[0] + ")");
    }

    private void waitForNextSnapshot(JobRepository jr, long jobId, int timeoutSeconds) {
        long originalSnapshotId = jr.getJobExecutionRecord(jobId).snapshotId();
        // wait until there is at least one more snapshot
        long[] snapshotId = {-1};
        assertTrueEventually(() -> {
            JobExecutionRecord record = jr.getJobExecutionRecord(jobId);
            assertNotNull("jobExecutionRecord is null", record);
            snapshotId[0] = record.snapshotId();
            assertTrue("No more snapshots produced after restart in " + timeoutSeconds + " seconds",
                    snapshotId[0] > originalSnapshotId);
            assertTrue("stats are 0", record.snapshotStats().numBytes() > 0);
        }, timeoutSeconds);
        logger.info("Next snapshot found (id=" + snapshotId[0] + ", previous id=" + originalSnapshotId + ")");
    }

    @Test
    public void when_jobRestartedGracefully_then_noOutputDuplicated() {
        DAG dag = new DAG();
        int elementsInPartition = 100;
        DistributedSupplier<Processor> sup = () ->
                new SequencesInPartitionsGeneratorP(3, elementsInPartition, true);
        Vertex generator = dag.newVertex("generator", throttle(sup, 30))
                              .localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("sink"));
        dag.edge(between(generator, sink));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(3600_000); // set long interval so that the first snapshot does not execute
        Job job = instance1.newJob(dag, config);

        // wait for the job to start producing output
        List<Entry<Integer, Integer>> sinkList = instance1.getList("sink");
        assertTrueEventually(() -> assertTrue(sinkList.size() > 10));

        // When
        job.restart();
        job.join();

        // Then
        Set<Entry<Integer, Integer>> expected = IntStream.range(0, elementsInPartition)
                 .boxed()
                 .flatMap(i -> IntStream.range(0, 3).mapToObj(p -> entry(p, i)))
                 .collect(Collectors.toSet());
        assertEquals(expected, new HashSet<>(sinkList));
    }

    @Test
    public void stressTest_restart() throws Exception {
        stressTest(tuple -> {
            logger.info("restarting");
            tuple.f2().restart();
            logger.info("restarted");
            // Sleep a little in order to not observe the RUNNING status from the execution
            // before the restart.
            LockSupport.parkNanos(MILLISECONDS.toNanos(500));
            assertJobStatusEventually(tuple.f2(), JobStatus.RUNNING);
            return tuple.f2();
        });
    }

    @Test
    public void stressTest_suspendAndResume() throws Exception {
        stressTest(tuple -> {
            logger.info("Suspending the job...");
            tuple.f2().suspend();
            logger.info("suspend() returned");
            assertJobStatusEventually(tuple.f2(), JobStatus.SUSPENDED, 15);
            // The Job.resume() call might overtake the suspension.
            // resume() does nothing when job is not suspended. Without
            // the sleep, the job might remain suspended.
            sleepSeconds(1);
            logger.info("Resuming the job...");
            tuple.f2().resume();
            logger.info("resume() returned");
            assertJobStatusEventually(tuple.f2(), JobStatus.RUNNING, 15);
            return tuple.f2();
        });
    }

    @Test
    public void stressTest_cancelWithSnapshotAndResubmit() throws Exception {
        stressTest(tuple -> {
            logger.info("Cancelling the job with snapshot...");
            tuple.f2().cancelAndExportSnapshot("state");
            logger.info("cancel() returned");
            assertJobStatusEventually(tuple.f2(), JobStatus.FAILED, 15);
            logger.info("Resubmitting the job...");
            Job newJob = tuple.f0().newJob(tuple.f1(),
                    new JobConfig()
                            .setInitialSnapshotName("state")
                            .setProcessingGuarantee(EXACTLY_ONCE)
                            .setSnapshotIntervalMillis(10));
            logger.info("newJob() returned");
            assertJobStatusEventually(newJob, JobStatus.RUNNING, 15);
            return newJob;
        });
    }

    private void stressTest(Function<Tuple3<JetInstance, DAG, Job>, Job> action) throws Exception {
        JobRepository jobRepository = new JobRepository(instance1);
        TestProcessors.reset(2);

        DAG dag = new DAG();
        dag.newVertex("generator", DummyStatefulP::new)
           .localParallelism(1);

        Job[] job = {instance1.newJob(dag,
                new JobConfig().setSnapshotIntervalMillis(10)
                               .setProcessingGuarantee(EXACTLY_ONCE))};

        logger.info("waiting for 1st snapshot");
        waitForFirstSnapshot(jobRepository, job[0].getId(), 5);
        logger.info("first snapshot found");
        spawn(() -> {
            for (int i = 0; i < 10; i++) {
                job[0] = action.apply(tuple3(instance1, dag, job[0]));
                waitForNextSnapshot(jobRepository, job[0].getId(), 5);
            }
            return null;
        }).get();

        job[0].cancel();
        try {
            job[0].join();
            fail("CancellationException was expected");
        } catch (CancellationException expected) {
        }
    }

    /**
     * A source, that will generate integer sequences from 0..ELEMENTS_IN_PARTITION,
     * one sequence for each partition.
     * <p>
     * Generated items are {@code entry(partitionId, value)}.
     */
    static class SequencesInPartitionsGeneratorP extends AbstractProcessor {

        private final int numPartitions;
        private final int elementsInPartition;
        private final boolean assertJobRestart;

        private int[] assignedPtions;
        private int[] ptionOffsets;

        private int ptionCursor;
        private MyTraverser traverser;
        private Traverser<Entry<BroadcastKey<Integer>, Integer>> snapshotTraverser;
        private Entry<Integer, Integer> pendingItem;
        private boolean wasRestored;

        SequencesInPartitionsGeneratorP(int numPartitions, int elementsInPartition, boolean assertJobRestart) {
            this.numPartitions = numPartitions;
            this.elementsInPartition = elementsInPartition;
            this.assertJobRestart = assertJobRestart;

            this.traverser = new MyTraverser();
        }

        @Override
        protected void init(@Nonnull Context context) {
            this.assignedPtions = IntStream.range(0, numPartitions)
                                           .filter(i -> i % context.totalParallelism() == context.globalProcessorIndex())
                                           .toArray();
            assert assignedPtions.length > 0 : "no assigned partitions";
            this.ptionOffsets = new int[assignedPtions.length];

            getLogger().info("assignedPtions=" + Arrays.toString(assignedPtions));
        }

        @Override
        public boolean complete() {
            boolean res = emitFromTraverserInt(traverser);
            if (res) {
                assertTrue("Reached end of batch without restoring from a snapshot", wasRestored || !assertJobRestart);
            }
            return res;
        }

        @Override
        public boolean saveToSnapshot() {
            // finish emitting any pending item first before starting snapshot
            if (pendingItem != null) {
                if (tryEmit(pendingItem)) {
                    pendingItem = null;
                } else {
                    return false;
                }
            }
            if (snapshotTraverser == null) {
                snapshotTraverser = Traversers.traverseStream(IntStream.range(0, assignedPtions.length).boxed())
                                              // save {partitionId; partitionOffset} tuples
                                              .map(i -> entry(broadcastKey(assignedPtions[i]), ptionOffsets[i]))
                                              .onFirstNull(() -> snapshotTraverser = null);
                getLogger().info("Saving snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                        + Arrays.toString(assignedPtions));
            }
            return emitFromTraverserToSnapshot(snapshotTraverser);
        }

        @Override
        public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            BroadcastKey<Integer> bKey = (BroadcastKey<Integer>) key;
            int partitionIndex = arrayIndexOf(bKey.key(), assignedPtions);
            // restore offset, if assigned to us. Ignore it otherwise
            if (partitionIndex >= 0) {
                ptionOffsets[partitionIndex] = (int) value;
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            getLogger().info("Restored snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                    + Arrays.toString(assignedPtions));
            // we'll start at the most-behind partition
            advanceCursor();
            wasRestored = true;
            return true;
        }

        // this method is required to keep track of pending item
        private boolean emitFromTraverserInt(MyTraverser traverser) {
            Entry<Integer, Integer> item;
            if (pendingItem != null) {
                item = pendingItem;
                pendingItem = null;
            } else {
                item = traverser.next();
            }
            for (; item != null; item = traverser.next()) {
                if (!tryEmit(item)) {
                    pendingItem = item;
                    return false;
                }
            }
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
                try {
                    return ptionOffsets[ptionCursor] < elementsInPartition
                            ? entry(assignedPtions[ptionCursor], ptionOffsets[ptionCursor]) : null;
                } finally {
                    ptionOffsets[ptionCursor]++;
                    advanceCursor();
                }
            }
        }
    }

    /**
     * A source processor which never completes and only allows the first
     * snapshot to finish.
     */
    private static final class FirstSnapshotProcessor implements Processor {
        private boolean firstSnapshotDone;

        @Override
        public boolean complete() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            try {
                return !firstSnapshotDone;
            } finally {
                firstSnapshotDone = true;
            }
        }
    }
}
