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
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

/**
 * A processor that inserts watermark into a data stream. See
 * {@link com.hazelcast.jet.processor.Processors#insertWatermarks(
 *     DistributedToLongFunction,
 *     com.hazelcast.jet.function.DistributedSupplier,
 *     WindowDefinition) Processors.insertWatermarks()}.
 *
 * @param <T> type of the stream item
 */
public class InsertWatermarksP<T> extends AbstractProcessor {

    private static final Object NULL_OBJECT = new Object();

    private final ToLongFunction<T> getTimestampFn;
    private final WatermarkPolicy wmPolicy;
    private final WindowDefinition winDef;

    private final ResettableSingletonTraverser<Object> itemTraverser = new ResettableSingletonTraverser<>();
    private final WatermarkPerFrameTraverser wmTraverser = new WatermarkPerFrameTraverser();
    private final FlatMapper<Object, Object> flatMapper = flatMapper(this::traverserFor);

    private long lastEmittedWm = Long.MIN_VALUE;
    private long nextWm = Long.MIN_VALUE;
    private int globalProcessorIndex;

    /**
     * @param getTimestampFn function that extracts the timestamp from the item
     * @param wmPolicy the watermark policy
     * @param windowDefinition window definition to be used for watermark generation
     */
    public InsertWatermarksP(
            @Nonnull DistributedToLongFunction<T> getTimestampFn,
            @Nonnull WatermarkPolicy wmPolicy,
            WindowDefinition windowDefinition
    ) {
        this.getTimestampFn = getTimestampFn;
        this.wmPolicy = wmPolicy;
        this.winDef = windowDefinition;
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
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverserFor(Object item) {
        long proposedWm;
        if (item == NULL_OBJECT) {
            proposedWm = winDef.floorFrameTs(wmPolicy.getCurrentWatermark());
        } else {
            long eventTs = getTimestampFn.applyAsLong((T) item);
            proposedWm = winDef.floorFrameTs(wmPolicy.reportEvent(eventTs));

            // check for item lateness
            if (Math.max(proposedWm, lastEmittedWm) > eventTs) {
                logFine(getLogger(),"Dropped late event: %s", item);
            } else {
                itemTraverser.accept(item);
            }
        }

        if (proposedWm == wmTraverser.end) {
            return itemTraverser;
        }

        if (lastEmittedWm == Long.MIN_VALUE && proposedWm > lastEmittedWm) {
            lastEmittedWm = proposedWm - winDef.frameLength();
        }
        wmTraverser.end = proposedWm;
        return itemTraverser.prependTraverser(wmTraverser);
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
        logFine(getLogger(), "restored lastEmittedWm=%s", lastEmittedWm);
    }

    private class WatermarkPerFrameTraverser implements Traverser<Object> {

        long end; //inclusive

        @Override
        public Watermark next() {
            if (lastEmittedWm >= end) {
                return null;
            }
            if (nextWm < end) {
                nextWm = lastEmittedWm + winDef.frameLength();
            }
            if (nextWm <= end) {
                return new Watermark(lastEmittedWm = nextWm);
            }
            return null;
        }
    }
}
