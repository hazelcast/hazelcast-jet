package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.distinctP;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

/**
 * Javadoc pending.
 */
public class DistinctTransform<T, K> extends AbstractTransform {
    private final DistributedFunction<? super T, ? extends K> keyFn;

    public DistinctTransform(Transform upstream, DistributedFunction<? super T, ? extends K> keyFn) {
        super("distinct", upstream);
        this.keyFn = keyFn;
    }

    @Override
    public void addToDag(Planner p) {
        String namePrefix = p.uniqueVertexName(this.name(), "-step");
        Vertex v1 = p.dag.newVertex(namePrefix + '1', distinctP(keyFn))
                         .localParallelism(localParallelism());
        PlannerVertex pv2 = p.addVertex(this, namePrefix + '2', localParallelism(), distinctP(keyFn));
        p.addEdges(this, v1, (e, ord) -> e.partitioned(keyFn, HASH_CODE));
        p.dag.edge(between(v1, pv2.v).distributed().partitioned(keyFn));
    }
}
