/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static java.util.stream.Collectors.toList;

@RunWith(HazelcastSerialClassRunner.class)
public class AsyncTransformUsingContext_IntegrationTest extends JetTestSupport {

    @Test
    public void stressTest() {
        DAG dag = new DAG();
        List<Integer> input = IntStream.range(0, 10_000).boxed().collect(toList());
        Vertex source = dag.newVertex("source", ListSource.supplier(input));
        Vertex insWm = dag.newVertex("insWm", Processors.<Integer>insertWatermarksP(eventTimePolicy(
                i -> (long) i,
                WatermarkPolicy.limitingLag(10),
                10, 0, 0
        )));
        Vertex map = dag.newVertex("map", AsyncTransformUsingContextP2.supplier(
                ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8)).shareLocally(),
                (executor, item) -> {
                    CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
                    executor.submit(() -> f.complete(traverseItems(item + "-1", item + "-2")));
                    return f;
                },
                100,
                identity()
        )).localParallelism(2);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sinkList"));

        dag.edge(between(source, insWm))
           .edge(between(insWm, map))
           .edge(between(map, sink));

        JetInstance inst = createJetMember();
        Job job = inst.newJob(dag, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(0));
        job.join();

        IList<String> sinkList = inst.getList("sinkList");
        sinkList.forEach(System.out::println);
    }
}
