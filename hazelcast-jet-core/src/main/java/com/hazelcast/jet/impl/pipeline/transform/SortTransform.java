package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Comparator;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.Edge.between;


public class SortTransform<V> extends AbstractTransform {

    private static final String FIRST_STAGE_VERTEX_NAME_SUFFIX = "-prepare";
    private final FunctionEx<V, Long> keyFn;

    public SortTransform(Transform upstream, FunctionEx<V, Long> keyFn) {
        super("sort", upstream);
        this.keyFn = keyFn;
    }

    @Override
    public void addToDag(Planner p) {
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, Processors.sortPrepareP(keyFn))
                         .localParallelism(1);
        PlannerVertex pv2 = p.addVertex(this, name(), 1,
                ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(Processors.sortP())));
        p.addEdges(this, v1);
        p.dag.edge(between(v1, pv2.v).distributed().allToOne(name().hashCode()).monotonicOrder(new SerializableComparator<>(keyFn)));
    }

     static final class SerializableComparator<T> implements Comparator<Object>, Serializable {

        FunctionEx<T, Long> keyFn;

        SerializableComparator(FunctionEx<T, Long> keyFn) {
            this.keyFn = keyFn;
        }

        @Override
        public int compare(Object o1, Object o2) {
            long l1 = keyFn.apply((T) o1);
            long l2 = keyFn.apply((T) o2);
            return Long.compare(l1, l2);
        }
    }
}
