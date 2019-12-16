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
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class WriteObservableP<T> implements Processor {

    private final String name;
    private final HazelcastInstance instance;
    private final ILogger logger;

    private AtomicInteger pendingWrites = new AtomicInteger(0);
    private List<T> buffer;

    private WriteObservableP(String name, HazelcastInstance instance, ILogger logger) {
        this.name = name;
        this.instance = instance;
        this.logger = logger;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        buffer = new ArrayList<>();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(o -> buffer.add((T) o));

        if (!buffer.isEmpty()) {
            pendingWrites.incrementAndGet();
            ObservableRepository
                    .publishIntoObservable(buffer, name, instance)
                    .whenComplete(
                            (l, throwable) -> {
                                if (throwable != null) {
                                    logger.warning("Failed publishing into observable. Cause: " + throwable.getMessage()
                                            , throwable);
                                }
                                pendingWrites.decrementAndGet();
                            }
                    );
            buffer.clear();
        }
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

        private transient HazelcastInstance instance;
        private transient ILogger logger;

        Supplier(String name) {
            this.name = name;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.instance = context.jetInstance().getHazelcastInstance();
            this.logger = context.logger();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new WriteObservableP<T>(name, instance, logger))
                    .limit(count)
                    .collect(toList());
        }
    }

}
