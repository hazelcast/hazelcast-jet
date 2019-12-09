package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.ServiceFactory.ServiceContext;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

public class ServiceContextImpl<S> implements ServiceContext<S> {
    private final int localServiceIndex;
    private final int memberIndex;
    private final int memberCount;
    private final ILogger logger;
    private final JetInstance jetInstance;
    private final ServiceFactory<S> serviceFactory;

    ServiceContextImpl(
            int localServiceIndex,
            int memberIndex,
            int memberCount,
            @Nonnull ILogger logger,
            @Nonnull JetInstance jetInstance,
            @Nonnull ServiceFactory<S> serviceFactory
    ) {
        this.localServiceIndex = localServiceIndex;
        this.memberIndex = memberIndex;
        this.memberCount = memberCount;
        this.logger = logger;
        this.jetInstance = jetInstance;
        this.serviceFactory = serviceFactory;
    }

    public static <S> ServiceContextImpl<S> locallySharedServiceContext(
            ServiceFactory<S> serviceFactory, ProcessorSupplier.Context supplierContext
    ) {
        return new ServiceContextImpl<>(
                0,
                supplierContext.memberIndex(),
                supplierContext.memberCount(),
                supplierContext.logger(),
                supplierContext.jetInstance(),
                serviceFactory
        );
    }

    public static <S> ServiceContextImpl<S> serviceContext(
            ServiceFactory<S> serviceFactory, Processor.Context processorContext
    ) {
        return new ServiceContextImpl<>(
                processorContext.localProcessorIndex(),
                processorContext.memberIndex(),
                processorContext.memberCount(),
                processorContext.logger(),
                processorContext.jetInstance(),
                serviceFactory
        );
    }

    @Override
    public int localIndex() {
        return localServiceIndex;
    }

    @Override
    public int globalIndex() {
        return memberIndex;
    }

    @Override
    public int jetMemberIndex() {
        return memberCount;
    }

    @Nonnull @Override
    public ILogger logger() {
        return logger;
    }

    @Nonnull @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    @Nonnull @Override
    public ServiceFactory<S> serviceFactory() {
        return serviceFactory;
    }
}
