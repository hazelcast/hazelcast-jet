package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;
import com.hazelcast.jet.rocksdb.PrefixRocksMap.PrefixRocksMapIterator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class SortP<V> extends AbstractProcessor {

    private final List<PrefixRocksMapIterator> sortedMapsIterators = new ArrayList<>();
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

    private class ResultTraverser implements Traverser<Entry<Long, V>> {
        @Override
        @SuppressWarnings("unchecked")
        public Entry<Long, V> next() {
            Entry<Long, V> min = tuple2(Long.MAX_VALUE, null);
            PrefixRocksMapIterator minIterator = null;

            for (PrefixRocksMapIterator iterator : sortedMapsIterators) {
                if (!iterator.hasNext()) {
                    continue;
                }
                if ((Long) iterator.peek().getKey() <= min.getKey()) {
                    min = iterator.peek();
                    minIterator = iterator;
                }
            }
            if (minIterator == null) {
                return null;
            }
            minIterator.next();
            return min;
        }
    }
}