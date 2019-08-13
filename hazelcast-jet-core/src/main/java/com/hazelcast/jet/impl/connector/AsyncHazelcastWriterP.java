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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.connector.HazelcastWriters.handleInstanceNotActive;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.callbackOf;
import static com.hazelcast.jet.impl.util.Util.tryIncrement;

public abstract class AsyncHazelcastWriterP implements Processor {

    public static final int MAX_PARALLEL_ASYNC_OPS_DEFAULT = 1000;

    private final int maxParallelAsyncOps;
    private final AtomicInteger numConcurrentOps = new AtomicInteger();
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private final ExecutionCallback callback = callbackOf(
        response -> numConcurrentOps.decrementAndGet(),
        exception -> {
            numConcurrentOps.decrementAndGet();
            if (exception != null) {
                lastError.compareAndSet(null, exception);
            }
        });
    private final HazelcastInstance instance;
    private boolean isLocal;

    AsyncHazelcastWriterP(
        HazelcastInstance instance, int maxParallelAsyncOps, boolean isLocal
    ) {
        this.instance = instance;
        this.maxParallelAsyncOps = maxParallelAsyncOps;
        this.isLocal = isLocal;
    }

    @Override
    public final boolean tryProcess() {
        checkError();
        boolean result;
        try {
            result = processInternal();
        } catch (HazelcastInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal);
        }
        return result;
    }

    @Override
    public final void process(int ordinal, @Nonnull Inbox inbox) {
        checkError();
        try {
            processInternal(inbox);
        } catch (HazelcastInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal);
        }
    }

    @Override
    public final boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public final boolean saveToSnapshot() {
        return complete();
    }

    @Override
    public final boolean complete() {
        checkError();
        boolean result;
        try {
            result = completeInternal();
        } catch (HazelcastInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal);
        }
        return result && ensureAllWritten();
    }

    @CheckReturnValue
    protected boolean processInternal() {
        return true;
    }

    protected abstract void processInternal(Inbox inbox);

    @CheckReturnValue
    protected boolean completeInternal() {
        return true;
    }

    protected final void setCallback(ICompletableFuture future) {
        future.andThen(callback);
    }

    @CheckReturnValue
    protected final boolean tryAcquirePermit() {
        return tryIncrement(numConcurrentOps, 1, maxParallelAsyncOps);
    }

    /**
     * Acquires as many permits as we are able to immediately, up to
     * desiredNumber. Returns the number of actually acquired permits. Can
     * return 0.
     */
    @CheckReturnValue
    protected final int tryAcquirePermits(int desiredNumber) {
        int prev;
        int next;
        do {
            prev = numConcurrentOps.get();
            next = Math.min(prev + desiredNumber, maxParallelAsyncOps);
            if (next == prev) {
                return 0;
            }
        } while (!numConcurrentOps.compareAndSet(prev, next));
        return next - prev;
    }

    protected final HazelcastInstance instance() {
        return instance;
    }

    protected final boolean isLocal() {
        return isLocal;
    }

    private void checkError() {
        Throwable t = lastError.get();
        if (t instanceof HazelcastInstanceNotActiveException) {
            throw handleInstanceNotActive((HazelcastInstanceNotActiveException) t, isLocal);
        } else if (t != null) {
            throw sneakyThrow(t);
        }
    }

    private boolean ensureAllWritten() {
        boolean allWritten = numConcurrentOps.get() == 0;
        checkError();
        return allWritten;
    }

}
