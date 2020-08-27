package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.sortPrepareP;


public class SortTransform<V> extends AbstractTransform {

    private static final String FIRST_STAGE_VERTEX_NAME_SUFFIX = "-prepare";
    private final ComparatorEx<V> comparator;

    public SortTransform(Transform upstream, ComparatorEx<V> comparator) {
        super("sort", upstream);
        this.comparator = comparator;
    }

    @Override
    public void addToDag(Planner p) {
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, sortPrepareP(comparator));
        PlannerVertex pv2 = p.addVertex(this, name(), 1, ProcessorMetaSupplier
                .forceTotalParallelismOne(ProcessorSupplier.of(Processors.mapP(identity())), name()));
        p.addEdges(this, v1);
        p.dag.edge(between(v1, pv2.v).distributed().allToOne(name().hashCode())
                                     .monotonicOrder((ComparatorEx<Object>) comparator));
    }


}