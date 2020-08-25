package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;
import com.hazelcast.jet.rocksdb.PrefixRocksMap.Cursor;

import javax.annotation.Nonnull;

public class SortPrepareWithPersistenceP<V> extends AbstractProcessor {

    private final FunctionEx<? super V, ? extends Long> keyFn;
    private PrefixRocksMap<Long, V> rocksMap;
    private ResultTraverser resultTraverser;

    public SortPrepareWithPersistenceP(FunctionEx<? super V, ? extends Long> keyFn) {
        this.keyFn = keyFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        rocksMap = context.prefixStateBackend().getPrefixMap();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        Long key = keyFn.apply((V) item);
        rocksMap.add(key, (V) item);
        return true;
    }

    @Override
    public boolean complete() {
        if(rocksMap.isEmpty()) {
            return true;
        }
        if (resultTraverser == null) {
            rocksMap.compact();
            resultTraverser = new ResultTraverser();
        }
        return emitFromTraverser(resultTraverser);
    }

    private class ResultTraverser implements Traverser<V> {
        Cursor cursor = rocksMap.cursor();

        @Override
        public V next() {
            if (!cursor.advance()) {
                cursor.close();
                return null;
            }
            return (V) cursor.getEntry().getValue();
        }
    }
}
