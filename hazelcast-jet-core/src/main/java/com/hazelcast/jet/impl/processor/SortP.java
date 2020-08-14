package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;


public class SortP extends AbstractProcessor {

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        return tryEmit(item);
    }
}
