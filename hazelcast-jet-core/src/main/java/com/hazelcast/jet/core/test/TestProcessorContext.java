/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.core.test;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

/**
 * {@link Processor.Context} implementation suitable to be used in tests.
 */
public class TestProcessorContext extends TestProcessorSupplierContext implements Processor.Context {

    private int globalProcessorIndex;
    private ProcessingGuarantee processingGuarantee = ProcessingGuarantee.NONE;

    /**
     * Constructor with default values.
     */
    public TestProcessorContext() {
        globalProcessorIndex = 0;
    }

    @Override
    public int globalProcessorIndex() {
        return globalProcessorIndex;
    }

    /**
     * Set the global processor index
     */
    public TestProcessorContext setGlobalProcessorIndex(int globalProcessorIndex) {
        this.globalProcessorIndex = globalProcessorIndex;
        return this;
    }

    @Override
    public ProcessingGuarantee processingGuarantee() {
        return processingGuarantee;
    }

    /**
     * Sets the processing guarantee.
     */
    @Nonnull
    public TestProcessorContext setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
        return this;
    }

    @Nonnull @Override
    public TestProcessorContext setLogger(@Nonnull ILogger logger) {
        return (TestProcessorContext) super.setLogger(logger);
    }

    @Nonnull @Override
    public TestProcessorContext setJetInstance(@Nonnull JetInstance jetInstance) {
        return (TestProcessorContext) super.setJetInstance(jetInstance);
    }

    @Nonnull @Override
    public TestProcessorContext setTotalParallelism(int totalParallelism) {
        return (TestProcessorContext) super.setTotalParallelism(totalParallelism);
    }

    @Nonnull @Override
    public TestProcessorContext setLocalParallelism(int localParallelism) {
        return (TestProcessorContext) super.setLocalParallelism(localParallelism);
    }

    @Nonnull @Override
    public TestProcessorContext setVertexName(@Nonnull String vertexName) {
        return (TestProcessorContext) super.setVertexName(vertexName);
    }

    @Override
    protected String loggerName() {
        return vertexName() + "#" + globalProcessorIndex;
    }
}
