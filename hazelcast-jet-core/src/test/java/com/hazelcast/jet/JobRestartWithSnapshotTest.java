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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.processor.DiagnosticProcessors;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.TestUtil.throttle;
import static com.hazelcast.jet.impl.SnapshotRepository.SNAPSHOT_DATA_MAP_NAME_PREFIX;
import static com.hazelcast.jet.impl.SnapshotRepository.SNAPSHOT_RECORDS_MAP_NAME;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JobRestartWithSnapshotTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 4;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance;
    private JetInstance[] instances;
    private JetTestInstanceFactory factory;


    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        instances = factory.newMembers(config, NODE_COUNT);
        instance = instances[0];

    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot() throws InterruptedException {
        DAG dag = new DAG();
        DistributedSupplier<Processor> sup = () -> new StreamSource(100);
        Vertex generator = dag.newVertex("generator", throttle(sup, 1))
                              .localParallelism(1);
        Vertex logger = dag.newVertex("logger", DiagnosticProcessors.writeLogger())
                           .localParallelism(1);

        dag.edge(Edge.between(generator, logger));

        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(2000);
        Job job = instance.newJob(dag, config);

        int num = 0;
        while (true) {
            dumpSnapshots(instance, dag);
            Thread.sleep(5000);
            num++;
            if (num == 2) {
                instances[1].shutdown();
            }
         }

    }

    private static void dumpSnapshots(JetInstance jet, DAG dag) {
        IMap<List<Long>, SnapshotRecord> map = jet.getMap(SNAPSHOT_RECORDS_MAP_NAME);
        for (Entry<List<Long>, SnapshotRecord> entry : map.entrySet()) {
            List<Long> key = entry.getKey();
            long jobId = key.get(0);
            long snapshotId = key.get(1);

            SnapshotRecord sr = entry.getValue();
            System.out.println("---------- jobId=" + jobId + ", snapshotId=" + snapshotId);
            System.out.println("sr=" + sr);

            for (Vertex vertex : dag) {
                IStreamMap<Object, Object> map2 = jet.getMap(SNAPSHOT_DATA_MAP_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertex.getName());
                System.out.println("--- " + map2.getName());
                int cnt = 0;
                for (Entry<Object, Object> entry2 : map2.entrySet()) {
                    System.out.println(entry2);
                    cnt++;
                }
                System.out.println(cnt + " entries");
            }
        }
    }

    private IStreamMap<Object, Object> getSnapshot(Job job, long id) {
        String map = SnapshotRepository.snapshotDataMapName(job.getJobId(), id, "generator");
        return instance.getMap(map);
    }

    static class StreamSource extends AbstractProcessor {

        private final int end;
        private Traverser<Integer> traverser;
        private Integer lastEmitted = - 1;

        public StreamSource(int end) {
            this.end = end;
            this.traverser = getTraverser();
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser,  t -> {
               lastEmitted = t;
            });
        }

        @Override
        public boolean saveSnapshot() {
            System.out.println("Save snapshot");
            System.out.println("State:"  + lastEmitted);
            return tryEmitToSnapshot("next", lastEmitted + 1);
        }

        @Override
        public void restoreSnapshot(@Nonnull Inbox inbox) {
            lastEmitted = ((Map.Entry<Object, Integer>) inbox.poll()).getValue();
            traverser = getTraverser();
        }

        private Traverser<Integer> getTraverser() {
            return Traversers.traverseStream(IntStream.range(lastEmitted + 1, end).boxed());
        }

    }


}
