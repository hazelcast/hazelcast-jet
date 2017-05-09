package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.Function;

import javax.annotation.Nonnull;

/**
 * See {@link com.hazelcast.jet.Processors#writeSystemOut()}
 */
public class WriteSystemOutP extends AbstractProcessor {

    private Function<Object, String> toStringF;

    public WriteSystemOutP(Function<Object, String> toStringF) {
        this.toStringF = toStringF;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        getLogger().info(toStringF.apply(item));
        return true;
    }
}
