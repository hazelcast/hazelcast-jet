package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import java.io.Serializable;
import java.util.Comparator;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.sortPrepareP;


public class SortTransform<V> extends AbstractTransform {

    private static final String FIRST_STAGE_VERTEX_NAME_SUFFIX = "-prepare";
    private final Comparator<V> comparator;

    public SortTransform(Transform upstream, Comparator<V> comparator) {
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
                                     .monotonicOrder(new SerializableComparator<>(comparator)));
    }
    static final class SerializableComparator<T> implements Serializable, Comparator<Object> {
        Comparator<T> comparator;

        SerializableComparator(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Object o1, Object o2) {
            return comparator.compare((T) o1, (T) o2);
        }
    }

}