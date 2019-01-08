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
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
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
 * @param <K> extracted key type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingContextP2<C, T, K, R> extends AbstractProcessor {

    @Nonnull private final Function<T, K> extractKeyFn;
    // package-visible for test
    private C contextObject;

    private final ContextFactory<C> contextFactory;
    private final DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn;
    private final AtomicInteger asyncOpsCounter;
    private final int maxAsyncOps;

    private final Traverser<?> outputTraverser;
    private final ManyToOneConcurrentArrayQueue<Tuple3<T, Long, Traverser<Object>>> resultQueue;
    // TODO [viliam] use more efficient structure
    private final SortedMap<Long, Long> watermarkCounts = new TreeMap<>();
    private final Map<T, Integer> inFlightItems = new IdentityHashMap<>();
    private Traverser<Entry> snapshotTraverser;
    private boolean inComplete;

    private Long lastReceivedWm = Long.MIN_VALUE;
    private long lastEmittedWm = Long.MIN_VALUE;
    private long minRestoredWm = Long.MAX_VALUE;

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingContextP2(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn,
            @Nullable C contextObject,
            @Nonnull AtomicInteger asyncOpsCounter,
            int maxAsyncOps,
            @Nonnull Function<T, K> extractKeyFn
    ) {
        assert contextObject == null ^ contextFactory.isSharedLocally()
                : "if contextObject is shared, it must be non-null, or vice versa";

        this.contextFactory = contextFactory;
        this.callAsyncFn = callAsyncFn;
        this.contextObject = contextObject;
        this.asyncOpsCounter = asyncOpsCounter;
        this.maxAsyncOps = maxAsyncOps;
        this.extractKeyFn = extractKeyFn;

        resultQueue = new ManyToOneConcurrentArrayQueue<>(maxAsyncOps);
        outputTraverser = ((Traverser<Tuple3<T, Long, Traverser<Object>>>) resultQueue::poll)
                .flatMap(this::flatMapResultQueue);
    }

    private Traverser<Object> flatMapResultQueue(Tuple3<T, Long, Traverser<Object>> tuple) {
        inFlightItems.merge(tuple.f0(), -1, (o, n) -> o == 1 ? null : o - 1);
        Long count = watermarkCounts.merge(tuple.f1(), -1L, Long::sum);
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
            if (watermarkCounts.isEmpty() && lastReceivedWm > lastEmittedWm) {
                wmToEmit = lastReceivedWm;
            }
            if (wmToEmit > Long.MIN_VALUE) {
                lastEmittedWm = wmToEmit;
                return tuple.f2()
                            .append(new Watermark(wmToEmit));
            }
        }
        return tuple.f2();
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
        processItem((T) item);
        return true;
    }

    private void processItem(@Nonnull T item) {
        CompletableFuture<Traverser<R>> future = callAsyncFn.apply(contextObject, item);
        if (future == null) {
            return;
        }
        watermarkCounts.merge(lastReceivedWm, 1L, Long::sum);
        Long lastWatermarkAtReceiveTime = lastReceivedWm;
        // TODO [viliam] extract the action to a field to reduce GC litter
        future.thenAccept(e -> {
            boolean result = resultQueue.offer(tuple3(item, lastWatermarkAtReceiveTime, (Traverser<Object>) e));
            assert result : "resultQueue doesn't have sufficient capacity";
            asyncOpsCounter.decrementAndGet();
        });
        inFlightItems.merge(item, 1, Integer::sum);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        lastReceivedWm = watermark.timestamp();
        if (watermarkCounts.isEmpty()) {
            lastEmittedWm = watermark.timestamp();
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
        inComplete = true;
        return emitFromTraverser(outputTraverser) && watermarkCounts.isEmpty();
    }

    @Override
    public boolean saveToSnapshot() {
        if (inComplete) {
            // If we are in completing phase, we can have a half-emitted item. Instead of finishing it and
            // writing a snapshot, we finish the final items and save no state.
            return complete();
        }
        if (snapshotTraverser == null) {
            LoggingUtil.logFinest(getLogger(), "Saving to snapshot: %s, lastReceivedWm=%d", inFlightItems, lastReceivedWm);
            snapshotTraverser = traverseIterable(inFlightItems.entrySet())
                    .<Entry>map(en -> entry(
                            extractKeyFn.apply(en.getKey()),
                            tuple2(en.getKey(), en.getValue())))
                    .append(entry(broadcastKey(Keys.LAST_EMITTED_WM), lastReceivedWm))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            assert ((BroadcastKey) key).key().equals(Keys.LAST_EMITTED_WM) : "Unexpected key: " + key;
            // we restart at the oldest WM any instance was at at the time of snapshot
            minRestoredWm = Math.min(minRestoredWm, (long) value);
            return;
        }
        Tuple2<T, Integer> value1 = (Tuple2<T, Integer>) value;
        for (int i = 0; i < value1.f1(); i++) {
            LoggingUtil.logFinest(getLogger(), "Restored: %s", value1.f0());
            processItem(value1.f0());
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        lastReceivedWm = minRestoredWm;
        logFine(getLogger(), "restored lastReceivedWm=%s", minRestoredWm);
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

    private static final class Supplier<C, T, K, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final ContextFactory<C> contextFactory;
        private final DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn;
        private final int maxAsyncOps;
        private transient C contextObject;
        private transient AtomicInteger asyncOpsCounter = new AtomicInteger();
        private DistributedFunction<T, K> extractKeyFn;

        private Supplier(
                @Nonnull ContextFactory<C> contextFactory,
                @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn,
                int maxAsyncOps,
                DistributedFunction<T, K> extractKeyFn) {
            this.contextFactory = contextFactory;
            this.callAsyncFn = callAsyncFn;
            this.maxAsyncOps = maxAsyncOps;
            this.extractKeyFn = extractKeyFn;
        }

        @Override
        public void init(@Nonnull ProcessorSupplier.Context context) {
            if (contextFactory.isSharedLocally()) {
                contextObject = contextFactory.createFn().apply(context.jetInstance());
            }
            asyncOpsCounter = new AtomicInteger();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new AsyncTransformUsingContextP2<>(contextFactory, callAsyncFn, contextObject,
                    asyncOpsCounter, maxAsyncOps, extractKeyFn))
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
    public static <C, T, K, R> ProcessorSupplier supplier(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn,
            int maxAsyncOps,
            DistributedFunction<T, K> extractKeyFn
    ) {
        // TODO [viliam] make maxAsyncOps global value, not per member
        return new Supplier<>(contextFactory, callAsyncFn, maxAsyncOps, extractKeyFn);
    }

    private enum Keys {
        LAST_EMITTED_WM
    }
}
