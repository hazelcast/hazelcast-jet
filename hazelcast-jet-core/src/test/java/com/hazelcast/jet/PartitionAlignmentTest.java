/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Processors.listWriter;
import static com.hazelcast.jet.TestUtil.executeAndPeel;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class PartitionAlignmentTest {

    private static final int ITEM_COUNT = 32;
    private static final int PARTITION_COUNT = ITEM_COUNT / 2;

    private JetInstance instance;
    private JetTestInstanceFactory factory;

    @Before
    public void before() {
        factory = new JetTestInstanceFactory();
        final JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().setProperty("hazelcast.partition.count", String.valueOf(PARTITION_COUNT));
        instance = factory.newMembers(cfg, 2)[0];
    }

    @After
    public void after() {
        factory.shutdownAll();
    }


    @Test
    public void when_localAndDistributedEdges_thenPartitionsAligned() throws Throwable {
        final int localProcessorCount = PARTITION_COUNT / 4;
        final List<Integer> items = range(0, ITEM_COUNT).boxed().collect(toList());
        final SimpleProcessorSupplier supplierOfListProducer = () -> new ListProducer(items, items.size());
        final Partitioner partitioner = (item, partitionCount) -> (int) item % partitionCount;

        final Vertex distributedProducer = new Vertex("distributedProducer", supplierOfListProducer).localParallelism(1);
        final Vertex localProducer = new Vertex("localProducer", supplierOfListProducer).localParallelism(1);
        final Vertex processor = new Vertex("processor", Counter::new).localParallelism(localProcessorCount);
        final Vertex consumer = new Vertex("consumer", listWriter("numbers")).localParallelism(1);

        executeAndPeel(instance.newJob(new DAG()
                .addVertex(distributedProducer)
                .addVertex(localProducer)
                .addVertex(processor)
                .addVertex(consumer)
                .addEdge(new Edge(distributedProducer, processor).partitionedByCustom(partitioner).distributed())
                .addEdge(new Edge(localProducer, 0, processor, 1).partitionedByCustom(partitioner))
                .addEdge(new Edge(processor, consumer)))
        );
        assertEquals(
                items.stream()
                     .flatMap(i -> IntStream.of(0, 2)
                                            .mapToObj(count -> describe(i, new int[]{count, 1})))
                     .collect(toList()),
                instance.getHazelcastInstance().getList("numbers").stream().sorted().collect(toList())
        );
    }

    private static String describe(int i, int[] counts) {
        return String.format("%n%2d observed %s times", i, Arrays.toString(counts));
    }

    private static class Counter extends AbstractProcessor {
        // item -> [ordinal0_count, ordinal1_count]
        private final Map<Integer, int[]> counts = new HashMap<>();

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            counts.computeIfAbsent((Integer) item, x -> new int[2])[ordinal]++;
            return true;
        }

        @Override
        public boolean complete() {
            counts.entrySet().stream()
                  .forEach(e -> emit(describe(e.getKey(), e.getValue())));
            return true;
        }
    }
}
