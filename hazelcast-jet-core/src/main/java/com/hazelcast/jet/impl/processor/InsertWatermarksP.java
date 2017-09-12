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
import com.hazelcast.jet.ResettableSingletonTraverser;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkEmissionPolicy;
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.function.ToLongFunction;

/**
 * A processor that inserts watermark into a data stream. See
 * {@link com.hazelcast.jet.processor.Processors#insertWatermarks(
 *     DistributedToLongFunction,
 *     com.hazelcast.jet.function.DistributedSupplier,
 *     WatermarkEmissionPolicy) Processors.insertWatermarks()}.
 *
 * @param <T> type of the stream item
 */
public class InsertWatermarksP<T> extends AbstractProcessor {

    private static final Object NULL_OBJECT = new Object();

    private final ToLongFunction<T> getTimestampFn;
    private final WatermarkPolicy wmPolicy;
    private final WatermarkEmissionPolicy wmEmitPolicy;

    private final ResettableSingletonTraverser<Object> singletonTraverser = new ResettableSingletonTraverser<>();
    private final NextWatermarkTraverser nextWatermarkTraverser = new NextWatermarkTraverser();
    private final FlatMapper<Object, Object> flatMapper = flatMapper(this::traverser);

    private long lastEmittedWm = Long.MAX_VALUE;
    private long nextWm = Long.MIN_VALUE;
    private int globalProcessorIndex;

    /**
     * @param getTimestampFn function that extracts the timestamp from the item
     * @param wmPolicy the watermark policy
     */
    public InsertWatermarksP(
            @Nonnull DistributedToLongFunction<T> getTimestampFn,
            @Nonnull WatermarkPolicy wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy
    ) {
        this.getTimestampFn = getTimestampFn;
        this.wmPolicy = wmPolicy;
        this.wmEmitPolicy = wmEmitPolicy;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        globalProcessorIndex = context.globalProcessorIndex();
    }

    @Override
    public boolean tryProcess() {
        return flatMapper.tryProcess(NULL_OBJECT);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        if (lastEmittedWm == Long.MAX_VALUE) {
            lastEmittedWm = Long.MIN_VALUE;
        }
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverser(Object item) {
        long proposedWm;
        if (item == NULL_OBJECT) {
            proposedWm = wmPolicy.getCurrentWatermark();
        } else {
            long eventTs = getTimestampFn.applyAsLong((T) item);
            proposedWm = wmPolicy.reportEvent(eventTs);
            if (Math.max(proposedWm, lastEmittedWm) <= eventTs) {
                singletonTraverser.accept(item);
            } else {
                getLogger().fine("Dropped late event: " + item);
            }
        }
        if (proposedWm == Long.MIN_VALUE) {
            return singletonTraverser;
        }
        if (proposedWm < nextWm || nextWatermarkTraverser.limit > proposedWm) {
            return singletonTraverser;
        }
        nextWatermarkTraverser.limit = proposedWm;
        return singletonTraverser.prependTraverser(nextWatermarkTraverser);
    }

    @Override
    public boolean saveSnapshot() {
        return tryEmitToSnapshot(globalProcessorIndex, lastEmittedWm);
    }

    @Override
    public void restoreSnapshot(@Nonnull Inbox inbox) {
        // we restart at the oldest WM any instance was at at the time of snapshot
        for (Object o; (o = inbox.poll()) != null; ) {
            lastEmittedWm = Math.min(lastEmittedWm, ((Entry<?, Long>) o).getValue());
        }
        getLogger().info("restored lastEmittedWm=" + lastEmittedWm);
    }

    private class NextWatermarkTraverser implements Traverser<Watermark> {

        private long limit;

        @Override
        public Watermark next() {
            if (lastEmittedWm >= limit) {
                return null;
            }
            if (nextWm < limit) {
                nextWm = wmEmitPolicy.nextWatermark(lastEmittedWm, limit);
            }
            if (nextWm <= limit) {
                return new Watermark(lastEmittedWm = nextWm);
            }
            return null;
        }
    }
}
