package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.TreeMap;

public class SortPrepareP<V> extends AbstractProcessor {
    private final TreeMap<V, Object> map;
    private ResultTraverser resultTraverser;

    public SortPrepareP(ComparatorEx<V> comparator) {
        this.map = new TreeMap<>(comparator);
    }

    protected boolean tryProcess0(@Nonnull Object item) {
        map.put((V) item, null);
        return true;
    }

    @Override
    public boolean complete() {
        if (resultTraverser == null) {
            resultTraverser = new ResultTraverser();
        }
        return emitFromTraverser(resultTraverser);
    }

    private class ResultTraverser implements Traverser<V> {
        private final Iterator<V> iterator = map.keySet().iterator();

        @Override
        public V next() {
            if (!iterator.hasNext()) {
                return null;
            }
            try {
                return iterator.next();
            } finally {
                iterator.remove();
            }
        }
    }

}
