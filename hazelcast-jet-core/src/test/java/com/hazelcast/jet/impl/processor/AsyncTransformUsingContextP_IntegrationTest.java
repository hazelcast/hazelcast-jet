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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.IList;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class AsyncTransformUsingContextP_IntegrationTest extends JetTestSupport {

    @Parameter
    public boolean ordered;

    @Parameters(name = "ordered={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @Test
    public void stressTest() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig()
                .setMapName("sourceMap")
                .setCapacity(100_000));
        JetInstance inst = createJetMember(config);

        DAG dag = new DAG();
        IMapJet<Integer, Integer> sourceMap = inst.getMap("sourceMap");
        final int numItems = 10_000;
        sourceMap.putAll(IntStream.range(0, numItems).boxed().collect(toMap(i -> i, i -> i)));

        Vertex source = dag.newVertex("source", throttle(streamMapP("sourceMap", alwaysTrue(),
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, eventTimePolicy(
                        i -> (long) ((Integer) i),
                        WatermarkPolicy.limitingLag(10),
                        10, 0, 0
                )), 5000));
        Vertex map = dag.newVertex("map", getAsyncMapPSupplier()).localParallelism(2);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sinkList"));

        // Use shorter queue to not block the barrier from source for too long due to
        // backpressure from the slow mapper (in the edge to mapper).
        EdgeConfig edgeToMapperConfig = new EdgeConfig().setQueueSize(128);
        // Use shorter queue on output from mapper so that we experience backpressure
        // and stress-test it.
        EdgeConfig edgeFromMapperConfig = new EdgeConfig().setQueueSize(10);
        dag.edge(between(source, map).setConfig(edgeToMapperConfig))
           .edge(between(map, sink).setConfig(edgeFromMapperConfig));

        inst.newJob(dag, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE).setSnapshotIntervalMillis(0));

        IList<String> sinkList = inst.getList("sinkList");
        Set<String> expected = IntStream.range(0, numItems)
                                        .boxed()
                                        .flatMap(i -> Stream.of(i + "-1", i + "-2", i + "-3", i + "-4", i + "-5"))
                                        .collect(toSet());
        assertTrueEventually(() -> assertEquals(expected, new HashSet<>(sinkList)));
    }

    private ProcessorSupplier getAsyncMapPSupplier() {
        DistributedBiFunction<ExecutorService, Object, CompletableFuture<Traverser<String>>> mapFn = (executor, item) -> {
            CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
            executor.submit(() -> {
                // simulate random async call latency
                sleepMillis(ThreadLocalRandom.current().nextInt(5));
                return f.complete(traverseItems(item + "-1", item + "-2", item + "-3", item + "-4", item + "-5"));
            });
            return f;
        };
        ContextFactory<ExecutorService> contextFactory =
                ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8)).shareLocally();

        return ordered
                ? AsyncTransformUsingContextOrderedP.supplier(contextFactory, mapFn, 100)
                : AsyncTransformUsingContextUnorderedP.supplier(contextFactory, mapFn, 100, identity());
    }
}
