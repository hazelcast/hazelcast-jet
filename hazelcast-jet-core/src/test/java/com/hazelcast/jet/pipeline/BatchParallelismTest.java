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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
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
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class BatchParallelismTest {

    private static final int DEFAULT_PARALLELISM = 8;
    private static final int LOCAL_PARALLELISM = 11;
    private static final int UPSTREAM_PARALLELISM = 6;
    private static final Context PIPELINE_CTX = new Context() {
        @Override public int defaultLocalParallelism() {
            return DEFAULT_PARALLELISM;
        }
    };

    @Parameter(value = 0)
    public FunctionEx<BatchStage<Integer>, BatchStage<Integer>> transform;

    @Parameter(value = 1)
    public List<String> vertexNames;

    @Parameter(value = 2)
    public List<Integer> expectedLPs;

    @Parameter(value = 3)
    public String transformName;

    @Parameters(name = "{index}: transform={3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Collections.singletonList("map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "map"
                ),
                createParamSet(
                        stage -> stage
                                .flatMap(x -> Traversers.singleton(1))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Collections.singletonList("flat-map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM),
                        "flat-map"
                ),
                createParamSet(
                        stage -> stage
                                .sort()
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Arrays.asList("sort", "sort-collect"),
                        Arrays.asList(UPSTREAM_PARALLELISM, 1),
                        "sort"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Collections.singletonList("map-stateful-global"),
                        Collections.singletonList(1),
                        "map-stateful-global"
                ),
                createParamSet(
                        stage -> stage
                                .aggregate(AggregateOperations.counting())
                                .flatMap(x -> Traversers.<Integer>traverseItems())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Arrays.asList("aggregate-prepare", "aggregate"),
                        Arrays.asList(UPSTREAM_PARALLELISM, 1),
                        "aggregate"
                ),
                createParamSet(
                        stage -> stage
                                .distinct()
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Arrays.asList("distinct-prepare", "distinct"),
                        Arrays.asList(LOCAL_PARALLELISM, LOCAL_PARALLELISM),
                        "distinct"
                ),
                createParamSet(
                        stage -> stage
                                .rebalance()
                                .map(x -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        Collections.singletonList("map"),
                        Collections.singletonList(UPSTREAM_PARALLELISM), // TODO: Consider in more detail
                        "map-after-rebalance"
                )
        );
    }

    private static Object[] createParamSet(
            FunctionEx<BatchStage<Integer>, BatchStage<Integer>> transform,
            List<String> vertexNames,
            List<Integer> expectedLPs,
            String transformName
    ) {
        return new Object[]{transform, vertexNames, expectedLPs, transformName};
    }


    @Test
    public void when_transform_applied_lp_should_match_expectedLP() {
        // When
        DAG dag = applyTransformAndGetDag(transform);
        System.out.println(dag.toJson(DEFAULT_PARALLELISM));
        // Then
        for (int i = 0; i < vertexNames.size(); i++) {
            Vertex tsVertex = dag.getVertex(vertexNames.get(i));
            assertEquals((int) expectedLPs.get(i), tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
        }
    }

    private DAG applyTransformAndGetDag(FunctionEx<BatchStage<Integer>, BatchStage<Integer>> transform) {
        PipelineImpl p = (PipelineImpl) Pipeline.create();
        BatchStage<Integer> source = p.readFrom(TestSources.items(1))
                                       .setLocalParallelism(UPSTREAM_PARALLELISM);
        BatchStage<Integer> applied = source.apply(transform);
        applied.writeTo(Sinks.noop());
        return p.toDag(PIPELINE_CTX);
    }
}
