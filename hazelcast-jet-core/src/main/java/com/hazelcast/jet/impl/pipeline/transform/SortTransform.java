package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.PersistableTransform;

import java.io.Serializable;
import java.util.Comparator;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.sortPrepareP;
import static com.hazelcast.jet.core.processor.Processors.sortPrepareWithPersistenceP;


public class SortTransform<V> extends AbstractTransform implements PersistableTransform {

    private static final String FIRST_STAGE_VERTEX_NAME_SUFFIX = "-prepare";
    private final FunctionEx<V, Long> keyFn;
    private boolean usePersistence;

    public SortTransform(Transform upstream, FunctionEx<V, Long> keyFn) {
        super("sort", upstream);
        this.keyFn = keyFn;
    }

    @Override
    public void addToDag(Planner p) {
        SupplierEx<Processor> supplierEx;
        if(usePersistence) {
            supplierEx = sortPrepareWithPersistenceP(keyFn);
        }
        else {
            supplierEx = sortPrepareP(keyFn);
        }
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, supplierEx);
        PlannerVertex pv2 = p.addVertex(this, name(), 1, ProcessorMetaSupplier
                        .forceTotalParallelismOne(ProcessorSupplier.of(Processors.mapP(identity())), name()));
        p.addEdges(this, v1);
        p.dag.edge(between(v1, pv2.v).distributed().allToOne(name().hashCode())
                                     .monotonicOrder(new SerializableComparator<>(keyFn)));
    }

    @Override
    public void setUsePersistence(boolean usePersistence) {
        this.usePersistence = usePersistence;
    }

    static final class SerializableComparator<T> implements Comparator<Object>, Serializable {
        FunctionEx<T, Long> keyFn;

        SerializableComparator(FunctionEx<T, Long> keyFn) {
            this.keyFn = keyFn;
        }

        @Override
        public int compare(Object o1, Object o2) {
            return Long.compare(keyFn.apply((T) o1), keyFn.apply((T) o2));
        }
    }
}
