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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given async item-to-traverser function, using a
 * context object.
 * <p>
 * This processor keeps the order of input items: a stalling call for one item
 * will stall all subsequent items.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingContextP<C, T, R> extends AbstractProcessor {

    // package-visible for test
    C contextObject;

    private final ContextFactory<C> contextFactory;
    private final DistributedBiFunction<? super C, ? super T, CompletableFuture<? extends Traverser<? extends R>>>
            callAsyncFn;
    private final int maxAsyncOpsPerMember;

    private ArrayDeque<Object> queue;
    private Traverser<?> traverser;
    private int maxOps;
    private ResettableSingletonTraverser<Watermark> watermarkTraverser = new ResettableSingletonTraverser<>();

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingContextP(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<? extends Traverser<? extends R>>>
                    callAsyncFn,
            @Nullable C contextObject,
            int maxAsyncOpsPerMember
    ) {
        this.contextFactory = contextFactory;
        this.callAsyncFn = callAsyncFn;
        this.contextObject = contextObject;
        this.maxAsyncOpsPerMember = maxAsyncOpsPerMember;

        assert contextObject == null ^ contextFactory.isSharedLocally()
                : "if contextObject is shared, it must be non-null, or vice versa";
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!contextFactory.isSharedLocally()) {
            assert contextObject == null : "contextObject is not null: " + contextObject;
            contextObject = contextFactory.createFn().apply(context.jetInstance());
        }
        maxOps = maxAsyncOpsPerMember / context.localParallelism();
        if (maxOps < 1) {
            throw new JetException("maxOps=" + maxOps);
        }
        queue = new ArrayDeque<>(maxOps);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (queue.size() == maxOps) {
            // if queue is full, apply backpressure
            return false;
        }
        CompletableFuture<? extends Traverser<? extends R>> future = callAsyncFn.apply(contextObject, (T) item);
        if (future == null) {
            return true;
        }
        queue.add(future);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return queue.size() < maxOps
                && queue.add(watermark);
    }

    @Override
    public boolean tryProcess() {
        while (emitFromTraverser(traverser)) {
            // We check the futures in submission order. While this might increase latency for some
            // later-submitted item that gets the result before some earlier-submitted one, we don't
            // have to do many volatile reads to check all the futures in each call or a concurrent
            // queue. It's also easy to keep order of watermarks w.r.t. events.
            for (Object o; (o = queue.peek()) != null; ) {
                if (o instanceof Watermark) {
                    watermarkTraverser.accept((Watermark) o);
                    traverser = watermarkTraverser;
                }
                CompletableFuture<Traverser<? extends R>> f = (CompletableFuture<Traverser<? extends R>>) o;
                if (!f.isDone()) {
                    return true;
                }
                queue.remove();
                try {
                    traverser = f.get();
                } catch (Exception e) {
                    throw sneakyThrow(e);
                }
            }
        }
        // we return true - we can accept more items even while emitting this item
        return true;
    }

    @Override
    public boolean isCooperative() {
        if (!contextFactory.isCooperative()) {
            throw new JetException("Wrong processor type for this context");
        }
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
        private final DistributedBiFunction<? super C, ? super T, CompletableFuture<? extends Traverser<? extends R>>>
                callAsyncFn;
        private transient C contextObject;
        private int maxAsyncOps;

        private Supplier(
                @Nonnull ContextFactory<C> contextFactory,
                @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<? extends Traverser<? extends R>>>
                        callAsyncFn,
                int maxAsyncOps
        ) {
            this.contextFactory = contextFactory;
            this.callAsyncFn = callAsyncFn;
            this.maxAsyncOps = maxAsyncOps;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (contextFactory.isSharedLocally()) {
                contextObject = contextFactory.createFn().apply(context.jetInstance());
            }
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new AsyncTransformUsingContextP<>(contextFactory, callAsyncFn, contextObject,
                                maxAsyncOps))
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
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<? extends Traverser<? extends R>>>
                    callAsyncFn,
            int maxAsyncOps
    ) {
        return new Supplier<>(contextFactory, callAsyncFn, maxAsyncOps);
    }
}
