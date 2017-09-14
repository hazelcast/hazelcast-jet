/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyMap;

/**
 * Handles various setups of sliding and tumbling window aggregation.
 * See {@link com.hazelcast.jet.processor.Processors} for more documentation.
 *
 * @param <T> type of input item (stream item in 1st stage, Frame, if 2nd stage)
 * @param <A> type of the frame accumulator object
 * @param <R> type of the finished result
 */
public class SlidingWindowP<T, A, R> extends AbstractProcessor {

    // package-visible for testing
    final Map<Long, Map<Object, A>> tsToKeyToAcc = new HashMap<>();
    Map<Object, A> slidingWindow;

    private final WindowDefinition wDef;
    private final DistributedToLongFunction<? super T> getFrameTsFn;
    private final Function<? super T, ?> getKeyFn;
    private final AggregateOperation1<? super T, A, R> aggrOp;
    private final boolean isLastStage;

    private final FlatMapper<Watermark, ?> wmFlatMapper;

    private final A emptyAcc;
    private Traverser<Object> flushTraverser;
    private Traverser<Entry<SnapshotKey, A>> snapshotTraverser;

    // These two fields track the upper and lower bounds for the keyset of
    // tsToKeyToAcc. They serve as an optimization that avoids a linear search
    // over the entire keyset.
    private long topTs = Long.MIN_VALUE;
    private long bottomTs = Long.MAX_VALUE;

    public SlidingWindowP(
            Function<? super T, ?> getKeyFn,
            DistributedToLongFunction<? super T> getFrameTsFn,
            WindowDefinition winDef,
            AggregateOperation1<? super T, A, R> aggrOp,
            boolean isLastStage
    ) {
        if (!winDef.isTumbling()) {
            checkNotNull(aggrOp.combineFn(), "AggregateOperation lacks the combine primitive");
        }
        this.wDef = winDef;
        this.getFrameTsFn = getFrameTsFn;
        this.getKeyFn = getKeyFn;
        this.aggrOp = aggrOp;
        this.isLastStage = isLastStage;
        this.wmFlatMapper = flatMapper(wm -> windowTraverserAndEvictor(wm.timestamp()).append(wm));
        this.emptyAcc = aggrOp.createFn().get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        @SuppressWarnings("unchecked")
        T t = (T) item;
        final long frameTs = getFrameTsFn.applyAsLong(t);
        assert frameTs == wDef.floorFrameTs(frameTs) : "getFrameTsFn returned an invalid frame timestamp";
        final Object key = getKeyFn.apply(t);
        A acc = tsToKeyToAcc.computeIfAbsent(frameTs, x -> new HashMap<>())
                            .computeIfAbsent(key, k -> aggrOp.createFn().get());
        aggrOp.accumulateFn().accept(acc, t);
        topTs = max(topTs, frameTs);
        bottomTs = min(bottomTs, frameTs);
        return true;
    }

    @Override
    protected boolean tryProcessWm0(@Nonnull Watermark wm) {
        return wmFlatMapper.tryProcess(wm);
    }

    @Override
    public boolean complete() {
        return flushBuffers();
    }

    @Override
    public boolean saveSnapshot() {
        if (!isLastStage) {
            return flushBuffers();
        }
        ensureSnapshotTraverser();
        return emitSnapshotFromTraverser(snapshotTraverser);
    }

    @Override
    public void restoreSnapshot(@Nonnull Inbox inbox) {
        for (Object o; (o = inbox.poll()) != null; ) {
            @SuppressWarnings("unchecked")
            Entry<SnapshotKey, A> entry = (Entry<SnapshotKey, A>) o;
            SnapshotKey k = entry.getKey();
            if (tsToKeyToAcc.computeIfAbsent(k.timestamp, x -> new HashMap<>())
                            .put(k.key, entry.getValue()) != null) {
                throw new JetException("Duplicate key in snapshot: " + k);
            }
            topTs = max(topTs, k.timestamp);
            bottomTs = min(bottomTs, k.timestamp);
        }
    }

    private Traverser<Object> windowTraverserAndEvictor(long endTsExclusive) {
        assert endTsExclusive == wDef.floorFrameTs(endTsExclusive) : "endTsExclusive not aligned on frame";
        if (bottomTs == Long.MAX_VALUE) {
            bottomTs = endTsExclusive + wDef.frameLength();
            return Traversers.empty();
        }
        // compute the first window that needs to be emitted
        long rangeStart = min(bottomTs + wDef.windowLength() - wDef.frameLength(), endTsExclusive);
        return traverseStream(range(rangeStart, endTsExclusive, wDef.frameLength()).boxed())
                .flatMap(window -> traverseIterable(computeWindow(window).entrySet())
                        .map(e -> new TimestampedEntry<>(window, e.getKey(), aggrOp.finishFn().apply(e.getValue())))
                        .onFirstNull(() -> completeWindow(window)));
    }

    private Map<Object, A> computeWindow(long frameTs) {
        assert bottomTs >= frameTs - wDef.windowLength() + wDef.frameLength()
                : String.format("probably missed a WM or received a late event, "
                        + "bottomTs=%d, frameTs=%d, windowLength=%d, frameLength=%d",
                bottomTs, frameTs, wDef.windowLength(), wDef.frameLength());
        if (wDef.isTumbling()) {
            return tsToKeyToAcc.getOrDefault(frameTs, emptyMap());
        }
        if (aggrOp.deductFn() == null) {
            return recomputeWindow(frameTs);
        }
        if (slidingWindow == null) {
            slidingWindow = recomputeWindow(frameTs);
        } else {
            // add leading-edge frame
            patchSlidingWindow(aggrOp.combineFn(), tsToKeyToAcc.get(frameTs));
        }
        return slidingWindow;
    }

    private Map<Object, A> recomputeWindow(long frameTs) {
        Map<Object, A> window = new HashMap<>();
        for (long ts = frameTs - wDef.windowLength() + wDef.frameLength(); ts <= frameTs; ts += wDef.frameLength()) {
            tsToKeyToAcc.getOrDefault(ts, emptyMap())
                        .forEach((key, currAcc) -> aggrOp.combineFn().accept(
                                  window.computeIfAbsent(key, k -> aggrOp.createFn().get()),
                                  currAcc));
        }
        return window;
    }

    private void patchSlidingWindow(BiConsumer<? super A, ? super A> patchOp, Map<Object, A> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<Object, A> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                A result = acc != null ? acc : aggrOp.createFn().get();
                patchOp.accept(result, e.getValue());
                return result.equals(emptyAcc) ? null : result;
            });
        }
    }

    private void completeWindow(long frameTs) {
        long frameToEvict = frameTs - wDef.windowLength() + wDef.frameLength();
        Map<Object, A> evictedFrame = tsToKeyToAcc.remove(frameToEvict);
        bottomTs = frameToEvict + wDef.frameLength();
        if (!wDef.isTumbling() && aggrOp.deductFn() != null) {
            // deduct trailing-edge frame
            patchSlidingWindow(aggrOp.deductFn(), evictedFrame);
        }
    }

    private boolean flushBuffers() {
        if (flushTraverser == null) {
            if (tsToKeyToAcc.isEmpty()) {
                return true;
            }
            LongStream range = LongStream
                    .iterate(bottomTs + wDef.windowLength() - wDef.frameLength(), ts -> ts + wDef.frameLength())
                    .limit(max(0, 1 + (topTs - bottomTs) / wDef.frameLength()));
            flushTraverser = traverseStream(range.boxed())
                    .flatMap(this::windowTraverserAndEvictor)
                    .onFirstNull(() -> flushTraverser = null);
        }
        return emitFromTraverser(flushTraverser);
    }

    private void ensureSnapshotTraverser() {
        if (snapshotTraverser != null) {
            return;
        }
        snapshotTraverser = traverseIterable(tsToKeyToAcc.entrySet())
                .flatMap(e -> traverseIterable(e.getValue().entrySet())
                        .map(e2 -> entry(new SnapshotKey(e.getKey(), e2.getKey()), e2.getValue()))
                )
                .onFirstNull(() -> snapshotTraverser = null);
    }

    /**
     * Returns a stream of {@code long}s:
     * {@code for (long i = start; i <= end; i += step) yield i;}
     */
    private static LongStream range(long start, long end, long step) {
        return start > end
                ? LongStream.empty()
                : LongStream.iterate(start, n -> n + step).limit(1 + (end - start) / step);
    }
}
