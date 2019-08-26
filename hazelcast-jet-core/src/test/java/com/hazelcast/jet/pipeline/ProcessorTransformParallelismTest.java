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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM;
import static com.hazelcast.jet.pipeline.ContextFactory.withCreateFn;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class ProcessorTransformParallelismTest {

    private static final int localParallelism = 11;

    @Parameter(value = 0)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> cooperativeAndHasExplicitLocalParallelismTransform;

    @Parameter(value = 1)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> cooperativeAndHasNoExplicitLocalParallelismTransform;

    @Parameter(value = 2)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> nonCooperativeAndHasExplicitLocalParallelism;

    @Parameter(value = 3)
    public FunctionEx<StreamStage<Integer>, StreamStage<Integer>> monCooperativeAndHasNoExplicitLocalParallelism;

    @Parameter(value = 4)
    public String transformName;


    @Parameters(name = "{index}: transform={4}")
    @SuppressWarnings(value = {"checkstyle:LineLength", "checkstyle:MethodLength"})
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {{
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContext(withCreateFn(x -> null), (c, t) -> t)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContext(withCreateFn(x -> null), (c, t) -> t),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, t) -> t)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, t) -> t),
                        "mapUsingContext"
                }, {

                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContext(withCreateFn(x -> null), (c, k, t) -> t)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContext(withCreateFn(x -> null), (c, k, t) -> t),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> t)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> t),
                        "mapUsingPartitionedContext"
                }, {

                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContextAsync(withCreateFn(x -> null), (c, t) -> supplyAsync(() -> t))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContextAsync(withCreateFn(x -> null), (c, t) -> supplyAsync(() -> t)),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, t) -> supplyAsync(() -> t))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .mapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, t) -> supplyAsync(() -> t)),
                        "mapUsingContextAsync"
                }, {

                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContextAsync(withCreateFn(x -> null), (c, k, t) -> supplyAsync(() -> t))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContextAsync(withCreateFn(x -> null), (c, k, t) -> supplyAsync(() -> t)),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> supplyAsync(() -> t))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .mapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> supplyAsync(() -> t)),
                        "mapUsingPartitionedContextAsync"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContext(withCreateFn(x -> null), (c, t) -> false)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContext(withCreateFn(x -> null), (c, t) -> false),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, t) -> false)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, t) -> false),
                        "filterUsingContext"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContext(withCreateFn(x -> null), (c, k, t) -> false)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContext(withCreateFn(x -> null), (c, k, t) -> false),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> false)
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> false),
                        "filterUsingPartitionedContext"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContextAsync(withCreateFn(x -> null), (c, t) -> supplyAsync(() -> false))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContextAsync(withCreateFn(x -> null), (c, t) -> supplyAsync(() -> false)),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, t) -> supplyAsync(() -> false))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .filterUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, t) -> supplyAsync(() -> false)),
                        "filterUsingContextAsync"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContextAsync(withCreateFn(x -> null), (c, k, t) -> supplyAsync(() -> false))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContextAsync(withCreateFn(x -> null), (c, k, t) -> supplyAsync(() -> false)),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> supplyAsync(() -> false))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .filterUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> supplyAsync(() -> false)),
                        "filterUsingPartitionedContextAsync"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContext(withCreateFn(x -> null), (c, t) -> Traversers.<Integer>empty())
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContext(withCreateFn(x -> null), (c, t) -> Traversers.<Integer>empty()),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, t) -> Traversers.<Integer>empty())
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, t) -> Traversers.<Integer>empty()),
                        "flatMapUsingContext"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContext(withCreateFn(x -> null), (c, k, t) -> Traversers.<Integer>empty())
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContext(withCreateFn(x -> null), (c, k, t) -> Traversers.<Integer>empty()),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> Traversers.<Integer>empty())
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContext(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> Traversers.<Integer>empty()),
                        "flatMapUsingPartitionedContext"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContextAsync(withCreateFn(x -> null), (c, t) -> supplyAsync(Traversers::<Integer>empty))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContextAsync(withCreateFn(x -> null), (c, t) -> supplyAsync(Traversers::<Integer>empty)),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, t) -> supplyAsync(Traversers::<Integer>empty))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .flatMapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, t) -> supplyAsync(Traversers::<Integer>empty)),

                        "flatMapUsingContextAsync"
                }, {
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContextAsync(withCreateFn(x -> null), (c, k, t) -> supplyAsync(Traversers::<Integer>empty))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContextAsync(withCreateFn(x -> null), (c, k, t) -> supplyAsync(Traversers::<Integer>empty)),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> supplyAsync(Traversers::<Integer>empty))
                                        .setLocalParallelism(localParallelism),
                        (FunctionEx<StreamStage<Integer>, StreamStage<Integer>>)
                                stage -> stage
                                        .groupingKey(i -> i)
                                        .flatMapUsingContextAsync(withCreateFn(x -> null).toNonCooperative(), (c, k, t) -> supplyAsync(Traversers::<Integer>empty)),

                        "flatMapUsingPartitionedContextAsync"
                }
                });
    }

    @Test
    public void when_CooperativeAndHasExplicitLocalParallelism_then_UsesProvidedLP() {
        // When
        DAG dag = applyTransformAndGetDAG(cooperativeAndHasExplicitLocalParallelismTransform);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(localParallelism, tsVertex.getLocalParallelism());
    }


    @Test
    public void when_CooperativeAndHasNoExplicitLocalParallelism_then_UsesDefaultLP() {
        // When
        DAG dag = applyTransformAndGetDAG(cooperativeAndHasNoExplicitLocalParallelismTransform);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(LOCAL_PARALLELISM_USE_DEFAULT, tsVertex.getLocalParallelism());
    }

    @Test
    public void when_NonCooperativeAndHasExplicitLocalParallelism_then_UsesProvidedLP() {
        // When
        DAG dag = applyTransformAndGetDAG(nonCooperativeAndHasExplicitLocalParallelism);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(localParallelism, tsVertex.getLocalParallelism());
    }

    @Test
    public void when_NonCooperativeAndHasNoExplicitLocalParallelism_then_UsesDefaultLP() {
        // When
        DAG dag = applyTransformAndGetDAG(monCooperativeAndHasNoExplicitLocalParallelism);

        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM, tsVertex.getLocalParallelism());
    }

    private DAG applyTransformAndGetDAG(FunctionEx<StreamStage<Integer>, StreamStage<Integer>> transform) {
        Pipeline p = Pipeline.create();
        StreamStage<Integer> source = p.drawFrom(TestSources.items(1))
                                       .addTimestamps(t -> 0, 0);
        StreamStage<Integer> applied = source.apply(transform);
        applied.drainTo(Sinks.noop());
        return p.toDag();
    }

}
