/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.execution.init.ExecutionPlan.createLoggerName;

/**
 * Base class for processor wrappers. Delegates all calls to the wrapped
 * processor.
 */
public abstract class ProcessorWrapper implements Processor {

    private Processor wrapped;

    protected ProcessorWrapper(Processor wrapped) {
        this.wrapped = wrapped;
    }

    public Processor getWrapped() {
        return wrapped;
    }

    /**
     * Can be used only before any other method is called.
     */
    public void setWrapped(Processor wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean isCooperative() {
        return wrapped.isCooperative();
    }

    @Override
    public final void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        outbox = wrapOutbox(outbox);
        // Pass a logger with real class name to processor
        // We do this only if context is ProcCtx (that is, not for tests where TestProcessorContext can be used
        // and also other objects could be mocked or null, such as jetInstance())
        if (context instanceof ProcCtx) {
            ProcCtx c = (ProcCtx) context;
            NodeEngine nodeEngine = ((HazelcastInstanceImpl) c.jetInstance().getHazelcastInstance()).node.nodeEngine;
            ILogger newLogger = nodeEngine.getLogger(
                    createLoggerName(
                            getWrapped().getClass().getName(),
                            c.jobConfig().getName(),
                            c.vertexName(),
                            c.globalProcessorIndex())
            );
            context = new ProcCtx(c.jetInstance(), c.jobId(), c.executionId(), c.jobConfig(),
                    newLogger, c.vertexName(), c.localProcessorIndex(), c.globalProcessorIndex(), c.processingGuarantee(),
                    c.localParallelism(), c.memberIndex(), c.memberCount(), c.tempDirectories(), c.serializationService());
        }
        wrapped.init(outbox, context);
        initWrapper(outbox, context);
    }

    protected Outbox wrapOutbox(Outbox outbox) {
        return outbox;
    }

    protected void initWrapper(Outbox outbox, Context context) {
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        wrapped.process(ordinal, inbox);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return wrapped.tryProcessWatermark(watermark);
    }

    @Override
    public boolean tryProcess() {
        return wrapped.tryProcess();
    }

    @Override
    public boolean completeEdge(int ordinal) {
        return wrapped.completeEdge(ordinal);
    }

    @Override
    public boolean complete() {
        return wrapped.complete();
    }

    @Override
    public boolean saveToSnapshot() {
        return wrapped.saveToSnapshot();
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return wrapped.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        return wrapped.snapshotCommitFinish(success);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        wrapped.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean finishSnapshotRestore() {
        return wrapped.finishSnapshotRestore();
    }

    @Override
    public void close() throws Exception {
        wrapped.close();
    }
}
