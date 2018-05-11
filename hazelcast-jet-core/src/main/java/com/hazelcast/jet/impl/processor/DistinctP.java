package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Batch processor that emits the distinct items it observes. Items are
 * distinct if they have non-equal keys as returned from the supplied
 * {@code keyFn}.
 * */
public class DistinctP<T, K> extends AbstractProcessor {
    private final Function<? super T, ? extends K> keyFn;
    private final Set<K> seenItems = new HashSet<>();
    private boolean readyForNewItem = true;

    public DistinctP(Function<? super T, ? extends K> keyFn) {
        this.keyFn = keyFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return readyForNewItem && !seenItems.add(keyFn.apply((T) item)) || (readyForNewItem = tryEmit(item));
    }
}
