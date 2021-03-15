/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetQueryResultProducer;
import com.hazelcast.jet.sql.impl.JetSqlCoreBackendImpl;
import com.hazelcast.sql.impl.JetSqlCoreBackend;

import javax.annotation.Nonnull;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;

public final class RootResultConsumerSink implements Processor {

    private JetQueryResultProducer rootResultConsumer;
    private AtomicLong limit;

    private RootResultConsumerSink(AtomicLong limit) {
        this.limit = limit;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        HazelcastInstanceImpl hzInst = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
        JetSqlCoreBackendImpl jetSqlCoreBackend = hzInst.node.nodeEngine.getService(JetSqlCoreBackend.SERVICE_NAME);
        rootResultConsumer = jetSqlCoreBackend.getResultConsumerRegistry().remove(context.jobId());
        assert rootResultConsumer != null;
        if (limit != null) {
            rootResultConsumer.init(ExpressionUtil.limitFn(limit));
        }
    }

    @Override
    public boolean tryProcess() {
        rootResultConsumer.ensureNotDone();
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        rootResultConsumer.consume(inbox);
    }

    @Override
    public boolean complete() {
        rootResultConsumer.done();
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static ProcessorMetaSupplier rootResultConsumerSink(Address initiatorAddress, AtomicLong limit) {
        ProcessorSupplier pSupplier = ProcessorSupplier.of(() -> new RootResultConsumerSink(limit));
        return forceTotalParallelismOne(pSupplier, initiatorAddress);
    }
}
