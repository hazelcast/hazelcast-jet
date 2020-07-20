/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.TestProcessors.CollectPerProcessorSink;
import com.hazelcast.jet.core.TestProcessors.ListsSourceP;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class RoutingPolicyDistributedTest extends SimpleTestInClusterSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final List<Integer>[] NUMBERS = new List[]{
            IntStream.range(0, 4096).boxed().collect(toList()),
            IntStream.range(4096, 8192).boxed().collect(toList()),
            IntStream.range(8192, 12288).boxed().collect(toList()),
            IntStream.range(12288, 16385).boxed().collect(toList())
    };

    private CollectPerProcessorSink consumerSup;
    private static Address address0;
    private static Address address1;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
        address0 = instances()[0].getHazelcastInstance().getCluster().getLocalMember().getAddress();
        address1 = instances()[1].getHazelcastInstance().getCluster().getLocalMember().getAddress();
    }

    @Before
    public void before() {
        TestProcessors.reset(1);
        consumerSup = new CollectPerProcessorSink();

    }

    @Test
    public void when_distributedToOne() {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS);
        Vertex consumer = consumer(2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer).distributeTo(address1));

        instance().newJob(dag).join();

        assertEquals(0, consumerSup.getListAt(0).size());
        assertEquals(0, consumerSup.getListAt(1).size());

        assertEquals(setOf(NUMBERS), setOf(consumerSup.getListAt(2), consumerSup.getListAt(3)));
    }

    @Test
    public void when_distributedToOne_and_targetMemberMissing() throws Exception {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS);
        Vertex consumer = consumer(2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer).distributeTo(new Address("1.2.3.4", 9999)));

        exception.expectMessage("target not member");
        instance().newJob(dag).join();
    }

    private Vertex consumer(int localParallelism) {
        return new Vertex("consumer", consumerSup).localParallelism(localParallelism);
    }

    private static Vertex producer(List<?>... lists) {
        assertEquals(0, lists.length % instances().length);
        return new Vertex("producer", new ListsSourceP(lists))
                .localParallelism(lists.length / instances().length);
    }

    @SafeVarargs
    private static <T> Set<T> setOf(Collection<T>... collections) {
        Set<T> set = new HashSet<>();
        int totalSize = 0;
        for (Collection<T> collection : collections) {
            set.addAll(collection);
            totalSize += collection.size();
        }
        assertEquals("there were some duplicates in the collections", totalSize, set.size());
        return set;
    }
}
