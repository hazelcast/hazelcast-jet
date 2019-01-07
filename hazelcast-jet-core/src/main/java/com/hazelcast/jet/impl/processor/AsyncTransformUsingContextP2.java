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
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.ContextFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.stream.Collectors.toList;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given async item-to-traverser function, using a
 * context object.
 * <p>
 * This processors might reorder items: results are emitted as they are
 * asynchronously delivered. However, this processor doesn't reorder items with
 * respect to the watermarks that followed them. That is, a watermark is
 * guaranteed to be emitted <i>after</i> results for all items that occurred
 * before it are emitted.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingContextP2<C, T, R> extends AbstractProcessor {

    // package-visible for test
    private C contextObject;

    private final ContextFactory<C> contextFactory;
    private final DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn;
    private final AtomicInteger asyncOpsCounter;
    private final int maxAsyncOps;

    private final Traverser<?> outputTraverser;
    private final ManyToOneConcurrentArrayQueue<Tuple2<Long, Traverser<Object>>> resultQueue;
    private Long lastReceivedWatermark = Long.MIN_VALUE;
    private long lastEmittedWatermark = Long.MIN_VALUE;
    // TODO [viliam] use more efficient structure
    private final SortedMap<Long, Long> watermarkCounts = new TreeMap<>();

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingContextP2(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn,
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
        outputTraverser = ((Traverser<Tuple2<Long, Traverser<Object>>>) resultQueue::poll)
                .flatMap(tuple -> {
                    Long count = watermarkCounts.merge(tuple.f0(), -1L, Long::sum);
                    assert count >= 0 : "count=" + count;
                    if (count == 0) {
                        long wmToEmit = Long.MIN_VALUE;
                        for (Iterator<Entry<Long, Long>> it = watermarkCounts.entrySet().iterator(); it.hasNext(); ) {
                            Entry<Long, Long> entry = it.next();
                            if (entry.getValue() != 0) {
                                wmToEmit = entry.getKey();
                                break;
                            } else {
                                it.remove();
                            }
                        }
                        if (watermarkCounts.isEmpty() && lastReceivedWatermark > lastEmittedWatermark) {
                            wmToEmit = lastReceivedWatermark;
                        }
                        if (wmToEmit > Long.MIN_VALUE) {
                            lastEmittedWatermark = wmToEmit;
                            return tuple.f1()
                                    .append(new Watermark(wmToEmit));
                        }
                    }
                    return tuple.f1();
                });
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!contextFactory.isSharedLocally()) {
            assert contextObject == null : "contextObject is not null: " + contextObject;
            contextObject = contextFactory.createFn().apply(context.jetInstance());
        }
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
        if (!emitFromTraverser(outputTraverser)
                || !Util.tryIncrement(asyncOpsCounter, 1, maxAsyncOps)) {
            return false;
        }
        CompletableFuture<Traverser<R>> future = callAsyncFn.apply(contextObject, (T) item);
        if (future == null) {
            return true;
        }
        watermarkCounts.merge(lastReceivedWatermark, 1L, Long::sum);
        Long lastWatermarkAtReceiveTime = lastReceivedWatermark;
        // TODO [viliam] extract the action to a field to reduce GC litter
        future.thenAccept(e -> {
            boolean result = resultQueue.offer(tuple2(lastWatermarkAtReceiveTime, (Traverser<Object>) e));
            assert result : "resultQueue doesn't have sufficient capacity";
            asyncOpsCounter.decrementAndGet();
        });
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        lastReceivedWatermark = watermark.timestamp();
        if (watermarkCounts.isEmpty()) {
            lastEmittedWatermark = watermark.timestamp();
            return tryEmit(watermark);
        }
        return true;
    }

    @Override
    public boolean tryProcess() {
        emitFromTraverser(outputTraverser);
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(outputTraverser) && watermarkCounts.isEmpty();
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
        private final DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn;
        private final int maxAsyncOps;
        private transient C contextObject;
        @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
        private transient AtomicInteger asyncOpsCounter = new AtomicInteger();

        private Supplier(
                @Nonnull ContextFactory<C> contextFactory,
                @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn,
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
            return Stream.generate(() -> new AsyncTransformUsingContextP2<>(contextFactory, callAsyncFn, contextObject,
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
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn,
            int maxAsyncOps
    ) {
        // TODO [viliam] make maxAsyncOps global value, not per member
        return new Supplier<>(contextFactory, callAsyncFn, maxAsyncOps);
    }
}
