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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.observer.ObservableBatch;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.function.Consumer;

public final class WriteObservableP<T> implements Processor {

    //atm we use topics to implement observable
    //todo: use reliable topic instead?
    //todo: default topic config ok?

    private final ITopic<ObservableBatch> topic;
    private final ArrayList<T> buffer = new ArrayList<>();
    private final Consumer<Object> inboxConsumer = item -> buffer.add((T) item);

    private WriteObservableP(
            @Nonnull HazelcastInstance instance,
            String name
    ) {
        this.topic = instance.getTopic(name);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(inboxConsumer);

        if (buffer.size() > 0) {
            Object[] items = buffer.toArray();
            topic.publish(new ObservableBatch(items));
            buffer.clear();
        }
    }

    @Override
    public boolean complete() {
        topic.publish(new ObservableBatch(true));
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        // we're a sink, no need to forward the watermarks
        return true;
    }

    public static ProcessorSupplier supplier(String name) {
        return new Supplier<>(name);
    }

    @Override
    public boolean isCooperative() {
        return false; //todo: can it be cooperative?
    }

    //todo: how to capture failures?

    private static final class Supplier<T> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;

        private Supplier(String name) {
            super(null);
            this.name = name;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance) {
            return new WriteObservableP<>(instance, name);
        }
    }
}
