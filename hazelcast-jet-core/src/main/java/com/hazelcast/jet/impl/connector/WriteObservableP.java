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
import com.hazelcast.jet.impl.observer.ObservableRepository;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;

import javax.annotation.Nonnull;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class WriteObservableP<T> implements Processor {

    private static final int ASYNC_OPS_LIMIT = 16;

    private final String ringbufferName;

    private Ringbuffer<Object> ringbuffer;
    private ILogger logger;
    private final AtomicInteger pendingWrites = new AtomicInteger(0);
    private BiConsumer<Long, Throwable> callback = this::onAddAllComplete;

    private InboxAsCollection inboxAsCollection = new InboxAsCollection();

    private WriteObservableP(String ringbufferName) {
        this.ringbufferName = ringbufferName;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        HazelcastInstance instance = context.jetInstance().getHazelcastInstance();
        this.logger = context.logger();
        this.ringbuffer = instance.getRingbuffer(ObservableRepository.getRingBufferName(ringbufferName));
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!Util.tryIncrement(pendingWrites, 1, ASYNC_OPS_LIMIT)) {
            return;
        }
        inboxAsCollection.inbox = inbox;
        ringbuffer.addAllAsync(inboxAsCollection, OverflowPolicy.OVERWRITE)
                .whenComplete(callback);
    }

    private void onAddAllComplete(Long result, Throwable throwable) {
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

    private static class InboxAsCollection extends AbstractCollection<Object> {
        private Inbox inbox;

        @Nonnull @Override
        public Iterator<Object> iterator() {
            // reset the inbox so that we can only iterate once. After iteration the inbox is empty.
            Inbox localInbox = inbox;
            if (localInbox == null) {
                throw new IllegalStateException("2nd iteration");
            }
            inbox = null;
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return localInbox.peek() != null;
                }

                @Override
                public Object next() {
                    return localInbox.poll();
                }
            };
        }

        @Override
        public int size() {
            return inbox.size();
        }
    }
}
