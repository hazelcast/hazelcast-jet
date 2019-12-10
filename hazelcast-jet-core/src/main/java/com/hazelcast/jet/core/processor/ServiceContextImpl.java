package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.ServiceFactory.ServiceContext;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

public class ServiceContextImpl implements ServiceContext {
    private final int memberCount;
    private final int memberIndex;
    private final int localIndex;
    private final boolean hasLocalSharing;
    private final boolean hasOrderedAsyncResponses;
    private final int maxPendingCallsPerProcessor;
    private final ILogger logger;
    private final JetInstance jetInstance;

    ServiceContextImpl(
            int memberCount,
            int memberIndex,
            int localIndex,
            boolean hasLocalSharing,
            boolean hasOrderedAsyncResponses,
            int maxPendingCallsPerProcessor,
            @Nonnull ILogger logger,
            @Nonnull JetInstance jetInstance
    ) {
        this.memberCount = memberCount;
        this.memberIndex = memberIndex;
        this.localIndex = localIndex;
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
                supplierContext.memberCount(),
                supplierContext.memberIndex(),
                0,
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
                processorContext.memberCount(),
                processorContext.memberIndex(),
                processorContext.localProcessorIndex(),
                serviceFactory.hasLocalSharing(),
                serviceFactory.hasOrderedAsyncResponses(),
                serviceFactory.maxPendingCallsPerProcessor(),
                processorContext.logger(),
                processorContext.jetInstance()
        );
    }

    @Override
    public int memberCount() {
        return memberCount;
    }

    @Override
    public int memberIndex() {
        return memberIndex;
    }

    @Override
    public int localIndex() {
        return localIndex;
    }

    @Override
    public boolean isSharedLocally() {
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
