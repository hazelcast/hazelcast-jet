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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.internal.util.concurrent.ManyToOneConcurrentArrayQueue;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given item-to-traverser function, using a context
 * object.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingContextP2<C, T, R> extends AbstractProcessor {

    // package-visible for test
    private C contextObject;

    private final ContextFactory<C> contextFactory;
    private final DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
            CompletableFuture<Traverser<? extends R>>> callAsyncFn;
    private final AtomicInteger asyncOpsCounter;
    private final int maxAsyncOps;

    private Traverser<? extends R> outputTraverser;
    private final ResettableSingletonTraverser<R> singletonTraverser = new ResettableSingletonTraverser<>();
    private final ManyToOneConcurrentArrayQueue<Traverser<? extends R>> resultQueue;

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingContextP2(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                    CompletableFuture<Traverser<? extends R>>> callAsyncFn,
            @Nullable C contextObject,
            @Nonnull AtomicInteger asyncOpsCounter,
            int maxAsyncOps
    ) {
        assert contextObject == null ^ contextFactory.isSharedLocally()
                : "if contextObject is shared, it must be non-null, or vice versa";

        this.contextFactory = contextFactory;
        this.callAsyncFn = callAsyncFn;
        this.contextObject = contextObject;
        this.asyncOpsCounter = asyncOpsCounter;
        this.maxAsyncOps = maxAsyncOps;

        resultQueue = new ManyToOneConcurrentArrayQueue<>(maxAsyncOps);
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!contextFactory.isSharedLocally()) {
            assert contextObject == null : "contextObject is not null: " + contextObject;
            contextObject = contextFactory.createFn().apply(context.jetInstance());
        }
        Traverser<Traverser<? extends R>> outerTraverser = resultQueue::poll;
        outputTraverser = outerTraverser.flatMap(t -> t);
    }

    @Override
    public boolean isCooperative() {
        if (!contextFactory.isCooperative()) {
            throw new JetException("Wrong processor created");
        }
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (!Util.tryIncrement(asyncOpsCounter, 1, maxAsyncOps)) {
            return false;
        }

        CompletableFuture<Traverser<? extends R>> future =
                callAsyncFn.apply(singletonTraverser, contextObject, (T) item);
        if (future == null) {
            return true;
        }
        future.thenAccept(e -> {
            boolean result = resultQueue.offer(e);
            assert result : "Offer failed, resultQueue doesn't have sufficient capacity";
            asyncOpsCounter.decrementAndGet();
        });
        emitFromTraverser(outputTraverser);
        return true;
    }

    @Override
    public boolean tryProcess() {
        emitFromTraverser(outputTraverser);
        return true;
    }

    @Override
    public void close() {
        // close() might be called even if init() was not called.
        // Only destroy the context if is not shared (i.e. it is our own).
        if (contextObject != null && !contextFactory.isSharedLocally()) {
            contextFactory.destroyFn().accept(contextObject);
        }
        contextObject = null;
    }

    private static final class Supplier<C, T, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final ContextFactory<C> contextFactory;
        private final DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                Traverser<? extends R>> callAsyncFn;
        private final int maxAsyncOps;
        private transient C contextObject;
        private transient AtomicInteger asyncOpsCounter = new AtomicInteger();

        private Supplier(
                @Nonnull ContextFactory<C> contextFactory,
                @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                        Traverser<? extends R>> callAsyncFn,
                int maxAsyncOps
        ) {
            this.contextFactory = contextFactory;
            this.callAsyncFn = callAsyncFn;
            this.maxAsyncOps = maxAsyncOps;
        }

        @Override
        public void init(@Nonnull ProcessorSupplier.Context context) {
            if (contextFactory.isSharedLocally()) {
                contextObject = contextFactory.createFn().apply(context.jetInstance());
            }
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new AsyncTransformUsingContextP2(contextFactory, callAsyncFn, contextObject,
                                    asyncOpsCounter, maxAsyncOps))
                         .limit(count)
                         .collect(toList());
        }

        @Override
        public void close(Throwable error) {
            if (contextObject != null) {
                contextFactory.destroyFn().accept(contextObject);
            }
        }
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code callAsyncFn}, it can be used if needed.
     */
    public static <C, T, R> ProcessorSupplier supplier(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                    Traverser<? extends R>> callAsyncFn,
            int maxAsyncOps
    ) {
        // TODO [viliam] make maxAsyncOps global value, not per member
        return new Supplier<>(contextFactory, callAsyncFn, maxAsyncOps);
    }
}
