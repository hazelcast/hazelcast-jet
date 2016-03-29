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

import com.hazelcast.core.IList;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.processors.CombinerProcessor;
import com.hazelcast.jet.processors.CountProcessor;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ListTest extends JetBaseTest {


    @Test @Repeat(100)
    public void listShufflingTest() throws Exception {

        JetBaseTest.initCluster(2);
        Application application = createApplication("listShufflingTest");

        IList<Integer> sourceList = SERVER.getList(randomName());
        IList targetList = SERVER.getList(randomName());

        try {
            DAG dag = createDAG();

            int CNT = 10000;

            for (int i = 0; i < CNT; i++) {
                sourceList.add(i);
            }

            Vertex counter = createVertex("counter", CountProcessor.Factory.class, 1);
            Vertex combiner = createVertex("combiner", CombinerProcessor.Factory.class, 1);
            addVertices(dag, counter, combiner);

            Edge edge = new EdgeImpl.EdgeBuilder("edge", counter, combiner)
                    .shuffling(true)
                    .shufflingStrategy(new IListBasedShufflingStrategy(targetList.getName()))
                    .build();

            addEdges(dag, edge);

            counter.addSourceList(sourceList.getName());
            combiner.addSinkList(targetList.getName());
            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            assertEquals(1, targetList.size());
            assertEquals(CNT, targetList.get(0));
        } finally {

            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }
}
