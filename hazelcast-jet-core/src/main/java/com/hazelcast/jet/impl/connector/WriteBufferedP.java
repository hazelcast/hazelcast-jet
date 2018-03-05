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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.CloseableProcessorSupplier;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Closeable;

public final class WriteBufferedP<B, T> implements Processor, Closeable {

    private final DistributedFunction<Context, B> newBufferFn;
    private final DistributedBiConsumer<B, T> addToBufferFn;
    private final DistributedConsumer<B> flushBufferFn;
    private final DistributedConsumer<B> disposeBufferFn;

    private B buffer;

    WriteBufferedP(
            @Nonnull DistributedFunction<Context, B> newBufferFn,
            @Nonnull DistributedBiConsumer<B, T> addToBufferFn,
            @Nonnull DistributedConsumer<B> flushBufferFn,
            @Nonnull DistributedConsumer<B> disposeBufferFn
    ) {
        this.newBufferFn = newBufferFn;
        this.addToBufferFn = addToBufferFn;
        this.flushBufferFn = flushBufferFn;
        this.disposeBufferFn = disposeBufferFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        buffer = newBufferFn.apply(context);
    }

    /**
     * This is private API. Call
     * {@link com.hazelcast.jet.core.processor.SinkProcessors#writeBufferedP
     * SinkProcessors.writeBuffered()} instead.
     */
    @Nonnull
    public static <B, T> ProcessorSupplier supplier(
            @Nonnull DistributedFunction<Context, B> newBufferFn,
            @Nonnull DistributedBiConsumer<B, T> addToBufferFn,
            @Nonnull DistributedConsumer<B> flushBufferFn,
            @Nonnull DistributedConsumer<B> disposeBufferFn
    ) {
        return CloseableProcessorSupplier.of(
                () -> new WriteBufferedP<>(newBufferFn, addToBufferFn, flushBufferFn, disposeBufferFn)
        );
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(item -> addToBufferFn.accept(buffer, (T) item));
        flushBufferFn.accept(buffer);
    }

    @Override
    public boolean complete() {
        close();
        return true;
    }

    public void close() {
        disposeBufferFn.accept(buffer);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

}
