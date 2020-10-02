package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traversers;
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

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class ParallelismTest {

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
    public String transformName;

    @Parameter(value = 2)
    public int expectedLP;


    @Parameters(name = "{index}: transform={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "map",
                        UPSTREAM_PARALLELISM
                ),
                createParamSet(
                        stage -> stage
                                .flatMap(x -> Traversers.singleton(1))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "flat-map",
                        UPSTREAM_PARALLELISM
                )
        );
    }

    private static Object[] createParamSet(
            FunctionEx<BatchStage<Integer>, BatchStage<Integer>> transform,
            String transformName,
            int expectedLP
    ) {
        return new Object[]{transform, transformName, expectedLP};
    }


    @Test
    public void when_transform_applied_lp_should_match_expectedLP() {
        // When
        DAG dag = applyTransformAndGetDag(transform);
        // Then
        Vertex tsVertex = dag.getVertex(transformName);
        assertEquals(expectedLP, tsVertex.determineLocalParallelism(DEFAULT_PARALLELISM));
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
