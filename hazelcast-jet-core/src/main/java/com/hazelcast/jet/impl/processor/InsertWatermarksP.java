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
import com.hazelcast.jet.ResettableSingletonTraverser;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkEmissionPolicy;
import com.hazelcast.jet.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
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

    private final ToLongFunction<T> getTimestampF;
    private final WatermarkPolicy wmPolicy;
    private final WatermarkEmissionPolicy wmEmitPolicy;

    private final ResettableSingletonTraverser<Object> singletonTraverser = new ResettableSingletonTraverser<>();
    private final NextWatermarkTraverser nextWatermarkTraverser = new NextWatermarkTraverser();
    private final FlatMapper<Object, Object> flatMapper = flatMapper(this::traverser);

    private long lastEmittedWm = Long.MIN_VALUE;

    /**
     * @param getTimestampF function that extracts the timestamp from the item
     * @param wmPolicy the watermark policy
     */
    public InsertWatermarksP(
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull WatermarkPolicy wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy
    ) {
        this.getTimestampF = getTimestampF;
        this.wmPolicy = wmPolicy;
        this.wmEmitPolicy = wmEmitPolicy;
    }

    @Override
    public boolean tryProcess() {
        return flatMapper.tryProcess(NULL_OBJECT);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverser(Object item) {
        long timestamp = item == NULL_OBJECT
                ? wmPolicy.getCurrentWatermark() : getTimestampF.applyAsLong((T) item);
        if (timestamp < lastEmittedWm) {
            // drop late event
            return Traversers.empty();
        }
        long newWm;
        if (item != NULL_OBJECT) {
            singletonTraverser.accept(item);
            newWm = wmPolicy.reportEvent(timestamp);
            if (lastEmittedWm == Long.MIN_VALUE) {
                lastEmittedWm = newWm - 1;
            }
        } else {
            newWm = timestamp;
        }
        if (newWm >= lastEmittedWm) {
            nextWatermarkTraverser.limit = newWm;
            return singletonTraverser.prependTraverser(nextWatermarkTraverser);
        }
        return singletonTraverser;
    }

    private class NextWatermarkTraverser implements Traverser<Watermark> {

        private long limit;

        @Override
        public Watermark next() {
            if (lastEmittedWm >= limit) {
                return null;
            }
            long nextWm = wmEmitPolicy.nextWatermark(lastEmittedWm, limit);
            if (nextWm <= limit) {
                return new Watermark(lastEmittedWm = nextWm);
            }
            return null;
        }
    }
}
