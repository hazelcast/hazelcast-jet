package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;

import javax.annotation.Nonnull;
import java.util.function.Function;

public class SortPrepareP<V> extends AbstractProcessor {

    private final FunctionEx<? super V, ? extends Long> keyFn;
    private PrefixRocksMap<Long, V> rocksMap;

    public SortPrepareP(FunctionEx<? super V, ? extends Long> keyFn) {
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
        rocksMap.compact();
        return tryEmit(rocksMap);
    }
}
