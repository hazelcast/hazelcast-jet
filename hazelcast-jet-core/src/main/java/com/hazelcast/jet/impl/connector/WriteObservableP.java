/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.observer.ObservableUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class WriteObservableP<T> implements Processor {

    private static final int ASYNC_OPS_LIMIT = 16;
    private static final int MAX_BATCH_SIZE = RingbufferProxy.MAX_BATCH_SIZE;

    private final String ringbufferName;
    private final List<T> batch = new ArrayList<>(MAX_BATCH_SIZE);

    private Ringbuffer<Object> ringbuffer;
    private ILogger logger;
    private final AtomicInteger pendingWrites = new AtomicInteger(0);


    private WriteObservableP(String ringbufferName) {
        this.ringbufferName = ringbufferName;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        HazelcastInstance instance = context.jetInstance().getHazelcastInstance();
        this.logger = context.logger();
        this.ringbuffer = instance.getRingbuffer(ObservableUtil.getRingbufferName(ringbufferName));
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (batch.isEmpty()) {
            drainInbox(inbox);
        }
        tryFlush();
    }

    private void drainInbox(@Nonnull Inbox inbox) {
        int drained = 0;
        for (Object item; (item = inbox.poll()) != null && drained < MAX_BATCH_SIZE; drained++) {
            batch.add((T) item);
        }
    }

    private void tryFlush() {
        if (!batch.isEmpty() && Util.tryIncrement(pendingWrites, 1, ASYNC_OPS_LIMIT)) {
            ringbuffer.addAllAsync(batch, OverflowPolicy.OVERWRITE)
                    .whenComplete(this::onFlushComplete);
            batch.clear();
        }
    }

    private void onFlushComplete(Long result, Throwable throwable) {
        if (throwable != null) {
            logger.warning("Failed publishing into observable: " + throwable, throwable);
        }
        pendingWrites.decrementAndGet();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        // we're a sink, no need to forward the watermarks
        return true;
    }

    @Override
    public boolean complete() {
        return pendingWrites.get() <= 0;
    }

    public static <T> ProcessorSupplier supplier(String name) {
        return new Supplier<T>(name);
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        private static final long serialVersionUID = -1;

        private final String name;

        Supplier(String name) {
            this.name = name;
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new WriteObservableP<T>(name))
                    .limit(count)
                    .collect(toList());
        }
    }
}
