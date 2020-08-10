package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;
import com.hazelcast.jet.rocksdb.PrefixRocksMap.Cursor;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public class SortP<V> extends AbstractProcessor {

    private final List<Cursor> sortedMapsCursors = new ArrayList<>();
    private ResultTraverser resultTraverser;

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        sortedMapsCursors.add(((PrefixRocksMap<Long, V>) item).cursor());
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
            Cursor minCursor = null;

            for (Cursor cursor : sortedMapsCursors) {
                if (!cursor.advance()) {
                    continue;
                }
                Entry<Long, V> e = cursor.getEntry();
                if (e.getKey() <= min.getKey()) {
                    min = e;
                    minCursor = cursor;
                }
            }
            if (minCursor == null) {
                return null;
            }
            minCursor.advance();
            return min.getValue();
        }
    }
}
