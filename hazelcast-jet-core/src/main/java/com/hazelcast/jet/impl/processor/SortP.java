package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public class SortP<V> extends AbstractProcessor {

    private final List<PrefixRocksMap<Long, V>.PrefixRocksMapIterator> sortedMapsIterators = new ArrayList<>();
    private ResultTraverser resultTraverser;

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        sortedMapsIterators.add(((PrefixRocksMap<Long, V>) item).iterator());
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
        @Override
        public V next() {
            Entry<Long, V> min = entry(Long.MAX_VALUE, null);
            PrefixRocksMap<Long, V>.PrefixRocksMapIterator minIterator = null;

            for (PrefixRocksMap<Long, V>.PrefixRocksMapIterator iterator : sortedMapsIterators) {
                if (!iterator.hasNext()) {
                    continue;
                }
                Entry<Long, V> e = iterator.peek();
                if (e.getKey() <= min.getKey()) {
                    min = e;
                    minIterator = iterator;
                }
            }
            if (minIterator == null) {
                return null;
            }
            minIterator.next();
            return min.getValue();
        }
    }
}
