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
import com.hazelcast.jet.impl.observer.ObservableRepository;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;

import java.util.ArrayList;
import java.util.List;

public final class WriteObservableP<T> extends AsyncHazelcastWriterP {

    private static final int MAX_PARALLEL_ASYNC_OPS = 1;
    private static final int MAX_BATCH_SIZE = RingbufferProxy.MAX_BATCH_SIZE;

    private final Ringbuffer<Object> ringbuffer;
    private final List<T> batch = new ArrayList<>(MAX_BATCH_SIZE);

    private WriteObservableP(String observableName, HazelcastInstance instance) {
        super(instance, MAX_PARALLEL_ASYNC_OPS);
        this.ringbuffer = instance.getRingbuffer(ObservableRepository.ringbufferName(observableName));
    }

    @Override
    protected void processInternal(Inbox inbox) {
        if (batch.size() < MAX_BATCH_SIZE) {
            inbox.drainTo(batch, MAX_BATCH_SIZE - batch.size());
        }
        tryFlush();
    }

    @Override
    protected boolean flushInternal() {
        return tryFlush();
    }

    private boolean tryFlush() {
        if (batch.isEmpty()) {
            return true;
        }
        if (!tryAcquirePermit()) {
            return false;
        }
        setCallback(ringbuffer.addAllAsync(batch, OverflowPolicy.OVERWRITE));
        batch.clear();
        return true;
    }

    public static final class Supplier extends AbstractHazelcastConnectorSupplier {

        private final String observableName;

        public Supplier(String observableName) {
            super(null);
            this.observableName = observableName;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance) {
            return new WriteObservableP<>(observableName, instance);
        }
    }

}
