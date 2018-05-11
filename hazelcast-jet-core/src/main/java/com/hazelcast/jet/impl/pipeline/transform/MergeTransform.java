package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static java.util.Arrays.asList;

public class MergeTransform<T> extends AbstractTransform {

    public MergeTransform(Transform upstream1, Transform upstream2) {
        super("merge", asList(upstream1, upstream2));
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(), mapP(identity()));
        p.addEdges(this, pv.v);
    }
}
