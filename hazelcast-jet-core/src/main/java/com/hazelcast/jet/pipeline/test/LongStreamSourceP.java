/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implements the {@link TestSources#longStream} source.
 *
 * @since 4.3
 */
public class LongStreamSourceP extends AbstractProcessor {

    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS = 5;
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long eventsPerSecond;
    private final ILogger logger = Logger.getLogger(LongStreamSourceP.class);
    private final long wmGranularity;
    private final long wmOffset;
    private long startNanoTime;
    private long globalProcessorIndex;
    private long totalParallelism;

    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);
    private long lastReportNanos;
    private long valueAtLastReport;
    private long lastCallNanos;
    private long valueToEmit;
    private long lastEmittedWm;
    private long nowNanoTime;

    LongStreamSourceP(
            long startTime,
            long eventsPerSecond,
            EventTimePolicy<? super Long> eventTimePolicy
    ) {
        this.wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
        this.wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
        this.startNanoTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
        this.eventsPerSecond = eventsPerSecond;
    }

    @Override
    protected void init(@Nonnull Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        valueToEmit = globalProcessorIndex;
        startNanoTime += valueToEmit * NANOS_PER_SECOND / eventsPerSecond;
        lastCallNanos = lastReportNanos = startNanoTime;
    }

    @Override
    public boolean complete() {
        nowNanoTime = System.nanoTime();
        emitEvents();
        detectAndReportHiccup();
        if (logger.isFineEnabled()) {
            reportThroughput();
        }
        return false;
    }

    private void emitEvents() {
        long emitValuesUpTo = (nowNanoTime - startNanoTime) * eventsPerSecond / NANOS_PER_SECOND;
        while (emitFromTraverser(traverser) && valueToEmit < emitValuesUpTo) {
            long timestampNanoTime = startNanoTime + valueToEmit * NANOS_PER_SECOND / eventsPerSecond;
            long timestamp = NANOSECONDS.toMillis(timestampNanoTime) - nanoTimeMillisToCurrentTimeMillis;
            traverser.append(jetEvent(timestamp, valueToEmit));
            valueToEmit += totalParallelism;
            if (timestamp >= lastEmittedWm + wmGranularity) {
                long wmToEmit = timestamp - (timestamp % wmGranularity) + wmOffset;
                traverser.append(new Watermark(wmToEmit));
                lastEmittedWm = wmToEmit;
            }
        }
    }

    private void detectAndReportHiccup() {
        long millisSinceLastCall = NANOSECONDS.toMillis(nowNanoTime - lastCallNanos);
        if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
            logger.info(String.format("*** Source #%d hiccup: %,d ms%n", globalProcessorIndex, millisSinceLastCall));
        }
        lastCallNanos = nowNanoTime;
    }

    private void reportThroughput() {
        long nanosSinceLastReport = nowNanoTime - lastReportNanos;
        if (nanosSinceLastReport < SECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS)) {
            return;
        }
        lastReportNanos = nowNanoTime;
        long localItemCountSinceLastReport = (valueToEmit - valueAtLastReport) / totalParallelism;
        valueAtLastReport = valueToEmit;
        logger.fine(String.format("p%d: %,.0f items/second",
                globalProcessorIndex,
                localItemCountSinceLastReport / ((double) nanosSinceLastReport / NANOS_PER_SECOND)));
    }

    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }

}
