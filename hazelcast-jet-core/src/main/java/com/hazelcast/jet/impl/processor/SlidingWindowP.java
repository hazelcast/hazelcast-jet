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

import com.hazelcast.core.PartitionAware;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
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
    private final DistributedToLongFunction<? super T> getFrameTimestampF;
    private final Function<? super T, ?> getKeyF;
    private final AggregateOperation1<? super T, A, R> aggrOp;
    private final boolean isLastStage;

    private final FlatMapper<Watermark, Object> flatMapper;

    private final A emptyAcc;
    private Traverser<Object> flushTraverser;
    private Traverser<Entry<SnapshotKey, A>> snapshotTraverser;

    // following fields are optimizations to maintain lowest and highest key in the tsToKeyToAcc map
    private long topTs = Long.MIN_VALUE;
    private long bottomTs = Long.MAX_VALUE;

    public SlidingWindowP(
            Function<? super T, ?> getKeyF,
            DistributedToLongFunction<? super T> getFrameTimestampF,
            WindowDefinition winDef,
            AggregateOperation1<? super T, A, R> aggrOp,
            boolean isLastStage
    ) {
        this.wDef = winDef;
        this.getFrameTimestampF = getFrameTimestampF;
        this.getKeyF = getKeyF;
        this.aggrOp = aggrOp;
        this.isLastStage = isLastStage;

        this.flatMapper = flatMapper(
                wm -> windowTraverserAndEvictor(wm.timestamp())
                            .append(wm));
        this.emptyAcc = aggrOp.createAccumulatorF().get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        final Long frameTimestamp = getFrameTimestampF.applyAsLong(t);
        assert frameTimestamp == wDef.floorFrameTs(frameTimestamp) : "timestamp not on the verge of a frame";
        final Object key = getKeyF.apply(t);
        A acc = tsToKeyToAcc.computeIfAbsent(frameTimestamp, x -> new HashMap<>())
                            .computeIfAbsent(key, k -> aggrOp.createAccumulatorF().get());
        aggrOp.accumulateItemF().accept(acc, t);
        topTs = Math.max(topTs, frameTimestamp);
        bottomTs = Math.min(bottomTs, frameTimestamp);
        return true;
    }

    @Override
    protected boolean tryProcessWm0(@Nonnull Watermark wm) {
        return flatMapper.tryProcess(wm);
    }

    @Override
    public boolean complete() {
        return flushBuffers();
    }

    private boolean flushBuffers() {
        if (flushTraverser == null) {
            if (tsToKeyToAcc.isEmpty()) {
                return true;
            }
            LongStream range = LongStream.iterate(bottomTs + wDef.windowLength() - wDef.frameLength(), ts -> ts + wDef.frameLength())
                                         .limit(Math.max(0, (topTs - bottomTs) / wDef.frameLength() + 1));
            flushTraverser = traverseStream(range.boxed())
                    .flatMap(this::windowTraverserAndEvictor)
                    .onFirstNull(() -> flushTraverser = null);
        }
        return emitFromTraverser(flushTraverser);
    }

    private Traverser<Object> windowTraverserAndEvictor(long endTsExclusive) {
        assert endTsExclusive == wDef.floorFrameTs(endTsExclusive) : "WM timestamp not on the verge of a frame";
        return traverseIterable(computeWindow(endTsExclusive).entrySet())
                .map(e -> (Object) new TimestampedEntry<>(
                        endTsExclusive, e.getKey(), aggrOp.finishAccumulationF().apply(e.getValue())))
                .onFirstNull(() -> completeWindow(endTsExclusive));
    }

    private Map<Object, A> computeWindow(long frameTs) {
        assert bottomTs >= frameTs - wDef.windowLength() + wDef.frameLength()
                : "probably missed a WM or received late event, bottomTs=" + bottomTs + ", frameTs=" + frameTs
                        + ", windowLength=" + wDef.windowLength() + ", frameLength=" + wDef.frameLength();
        if (wDef.isTumbling()) {
            return tsToKeyToAcc.getOrDefault(frameTs, emptyMap());
        }
        if (aggrOp.deductAccumulatorF() != null) {
            if (slidingWindow == null) {
                // compute initial slidingWindow
                slidingWindow = new HashMap<>();
                for (long ts = frameTs - wDef.windowLength() + wDef.frameLength(); ts < frameTs;
                        ts += wDef.frameLength()) {
                    patchSlidingWindow(aggrOp.combineAccumulatorsF(), tsToKeyToAcc.get(ts));
                }
            }
            // add leading-edge frame
            patchSlidingWindow(aggrOp.combineAccumulatorsF(), tsToKeyToAcc.get(frameTs));
            return slidingWindow;
        }
        // without deductF we have to recompute the window from scratch
        Map<Object, A> window = new HashMap<>();
        for (long ts = frameTs - wDef.windowLength() + wDef.frameLength(); ts <= frameTs; ts += wDef.frameLength()) {
            tsToKeyToAcc.getOrDefault(ts, emptyMap())
                        .forEach((key, currAcc) -> aggrOp.combineAccumulatorsF().accept(
                                  window.computeIfAbsent(key, k -> aggrOp.createAccumulatorF().get()),
                                  currAcc));
        }
        return window;
    }

    private void completeWindow(long frameTs) {
        long frameToEvict = frameTs - wDef.windowLength() + wDef.frameLength();
        Map<Object, A> evictedFrame = tsToKeyToAcc.remove(frameToEvict);
        // this could make bottomTs not exactly conform to the definition of being "lowest key in tsToKeyToAcc
        // map", because the next frame after the one we just removed might not have any items, but it's enough
        // for now.
        bottomTs = frameToEvict + wDef.frameLength();
        if (!wDef.isTumbling() && aggrOp.deductAccumulatorF() != null) {
            // deduct trailing-edge frame
            patchSlidingWindow(aggrOp.deductAccumulatorF(), evictedFrame);
        }
    }

    private void patchSlidingWindow(BiConsumer<? super A, ? super A> patchOp, Map<Object, A> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<Object, A> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                A result = acc != null ? acc : aggrOp.createAccumulatorF().get();
                patchOp.accept(result, e.getValue());
                return result.equals(emptyAcc) ? null : result;
            });
        }
    }

    @Override
    public boolean saveSnapshot() {
        if (isLastStage) {
            if (snapshotTraverser == null) {
                snapshotTraverser = traverseIterable(tsToKeyToAcc.entrySet())
                        .flatMap(entry -> traverseIterable(entry.getValue().entrySet())
                                .map(entry2 -> entry(new SnapshotKey(entry.getKey(), entry2.getKey()), entry2.getValue()))
                        )
                        .onFirstNull(() -> snapshotTraverser = null);
            }
            return emitSnapshotFromTraverser(snapshotTraverser);
        } else {
            return flushBuffers();
        }
    }

    @Override
    public void restoreSnapshot(@Nonnull Inbox inbox) {
        for (Object o; (o = inbox.poll()) != null; ) {
            Entry<SnapshotKey, A> entry = (Entry<SnapshotKey, A>) o;
            SnapshotKey k = entry.getKey();
            if (tsToKeyToAcc.computeIfAbsent(k.timestamp, x -> new HashMap<>())
                            .put(k.key, entry.getValue()) != null) {
                throw new JetException("Duplicate key in snapshot: " + k);
            }
            topTs = Math.max(topTs, k.timestamp);
            bottomTs = Math.min(bottomTs, k.timestamp);
        }
    }

    public static final class SnapshotKey implements PartitionAware<Object>, IdentifiedDataSerializable {
        private long timestamp;
        private Object key;

        public SnapshotKey() { }

        private SnapshotKey(long timestamp, @Nonnull Object key) {
            this.timestamp = timestamp;
            this.key = key;
        }

        @Override
        public Object getPartitionKey() {
            return key;
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.SLIDING_WINDOW_P_SNAPSHOT_KEY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(timestamp);
            out.writeObject(key);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            timestamp = in.readLong();
            key = in.readObject();
        }

        @Override
        public String toString() {
            return "SnapshotKey{timestamp=" + timestamp + ", key=" + key + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SnapshotKey that = (SnapshotKey) o;

            if (timestamp != that.timestamp) {
                return false;
            }
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + key.hashCode();
            return result;
        }
    }
}
