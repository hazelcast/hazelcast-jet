package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link ProcessorMetaSupplier}, which wraps another {@code
 * ProcessorMetaSupplier} with one, that will wrap its processors using
 * {@code wrapperSupplier}.
 */
public final class WrappingProcessorMetaSupplier implements ProcessorMetaSupplier {
    private ProcessorMetaSupplier wrapped;
    private Function<Processor, Processor> wrapperSupplier;

    public WrappingProcessorMetaSupplier(ProcessorMetaSupplier wrapped, Function<Processor,
            Processor> wrapperSupplier) {
        this.wrapped = wrapped;
        this.wrapperSupplier = wrapperSupplier;
    }

    @Nonnull
    @Override
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        Function<Address, ProcessorSupplier> function = wrapped.get(addresses);
        return address -> new WrappingProcessorSupplier(function.apply(address), wrapperSupplier);
    }

    @Override
    public void init(@Nonnull Context context) {
        wrapped.init(context);
    }
}
