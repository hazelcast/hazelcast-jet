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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.observer.ObservableUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class WriteObservableP<T> implements Processor {

    private static final int ASYNC_OPS_LIMIT = 1;
    private static final int MAX_BATCH_SIZE = RingbufferProxy.MAX_BATCH_SIZE;

    private final String observableName;
    private final List<T> batch = new ArrayList<>(MAX_BATCH_SIZE);
    private final AtomicInteger pendingWrites = new AtomicInteger(0);

    private Ringbuffer<Object> ringbuffer;
    private ILogger logger;


    private WriteObservableP(String observableName) {
        this.observableName = observableName;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        HazelcastInstance instance = context.jetInstance().getHazelcastInstance();
        this.logger = context.logger();
        this.ringbuffer = instance.getRingbuffer(ObservableUtil.getRingbufferName(observableName));
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (batch.isEmpty()) {
            inbox.drainTo(batch, MAX_BATCH_SIZE);
        }
        tryFlush();
    }

    @Override
    public boolean tryProcess() {
        return tryFlush();
    }

    @Override
    public boolean saveToSnapshot() {
        return tryFlush();
    }

    private boolean tryFlush() {
        if (batch.isEmpty()) {
            return true;
        }
        if (Util.tryIncrement(pendingWrites, 1, ASYNC_OPS_LIMIT)) {
            ringbuffer.addAllAsync(batch, OverflowPolicy.OVERWRITE)
                    .whenComplete(this::onFlushComplete);
            batch.clear();
            return true;
        } else {
            return false;
        }
    }

    private void onFlushComplete(Long lastSeq, Throwable throwable) {
        if (throwable != null) {
            logger.warning("Failed publishing into observable '" + observableName + "'", throwable);
            //TODO (PR-1729): extract observable name from ringbuffer name
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
        return tryFlush() && pendingWrites.get() <= 0;
    }

    public static SupplierEx<Processor> supplier(String name) {
        return () -> new WriteObservableP<>(name);
    }

}
