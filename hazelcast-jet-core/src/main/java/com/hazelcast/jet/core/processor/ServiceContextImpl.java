package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.ServiceFactory.ServiceContext;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

public class ServiceContextImpl implements ServiceContext {
    private final int localServiceIndex;
    private final int memberIndex;
    private final int memberCount;
    private final boolean isCooperative;
    private final boolean hasLocalSharing;
    private final boolean hasOrderedAsyncResponses;
    private final int maxPendingCallsPerProcessor;
    private final ILogger logger;
    private final JetInstance jetInstance;

    ServiceContextImpl(
            int localServiceIndex,
            int memberIndex,
            int memberCount,
            boolean isCooperative,
            boolean hasLocalSharing,
            boolean hasOrderedAsyncResponses,
            int maxPendingCallsPerProcessor,
            @Nonnull ILogger logger,
            @Nonnull JetInstance jetInstance
    ) {
        this.localServiceIndex = localServiceIndex;
        this.memberIndex = memberIndex;
        this.memberCount = memberCount;
        this.isCooperative = isCooperative;
        this.hasLocalSharing = hasLocalSharing;
        this.hasOrderedAsyncResponses = hasOrderedAsyncResponses;
        this.maxPendingCallsPerProcessor = maxPendingCallsPerProcessor;
        this.logger = logger;
        this.jetInstance = jetInstance;
    }

    public static ServiceContextImpl locallySharedServiceContext(
            ServiceFactory<?> serviceFactory, ProcessorSupplier.Context supplierContext
    ) {
        return new ServiceContextImpl(
                0,
                supplierContext.memberIndex(),
                supplierContext.memberCount(),
                serviceFactory.isCooperative(),
                serviceFactory.hasLocalSharing(),
                serviceFactory.hasOrderedAsyncResponses(),
                serviceFactory.maxPendingCallsPerProcessor(),
                supplierContext.logger(),
                supplierContext.jetInstance()
        );
    }

    public static ServiceContextImpl serviceContext(
            ServiceFactory<?> serviceFactory, Processor.Context processorContext
    ) {
        return new ServiceContextImpl(
                processorContext.localProcessorIndex(),
                processorContext.memberIndex(),
                processorContext.memberCount(),
                serviceFactory.isCooperative(),
                serviceFactory.hasLocalSharing(),
                serviceFactory.hasOrderedAsyncResponses(),
                serviceFactory.maxPendingCallsPerProcessor(),
                processorContext.logger(),
                processorContext.jetInstance()
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

    @Override
    public boolean isCooperative() {
        return isCooperative;
    }

    @Override
    public boolean hasLocalSharing() {
        return hasLocalSharing;
    }

    @Override
    public boolean hasOrderedAsyncResponses() {
        return hasOrderedAsyncResponses;
    }

    @Override
    public int maxPendingCallsPerProcessor() {
        return maxPendingCallsPerProcessor;
    }

    @Nonnull @Override
    public ILogger logger() {
        return logger;
    }

    @Nonnull @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

}
