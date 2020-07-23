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

    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS = 10;

    private static final long REPORT_PERIOD_NANOS = SECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS);
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;
    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long startTime;
    private final long itemsPerSecond;
    private final ILogger logger = Logger.getLogger(LongStreamSourceP.class);
    private final long wmGranularity;
    private final long wmOffset;
    private long globalProcessorIndex;
    private long totalParallelism;
    private long emitPeriod;

    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);
    private long emitSchedule;
    private long lastReport;
    private long counterAtLastReport;
    private long lastCallNanos;
    private long counter;
    private long lastEmittedWm;
    private long nowNanos;

    LongStreamSourceP(
            long startTime,
            long itemsPerSecond,
            EventTimePolicy<? super Long> eventTimePolicy
    ) {
        this.wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
        this.wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
        this.startTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
        this.itemsPerSecond = itemsPerSecond;
    }

    @Override
    protected void init(@Nonnull Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        emitPeriod = SECONDS.toNanos(1) * totalParallelism / itemsPerSecond;
        lastCallNanos = lastReport = emitSchedule =
                startTime + SECONDS.toNanos(1) * globalProcessorIndex / itemsPerSecond;
    }

    @Override
    public boolean complete() {
        nowNanos = System.nanoTime();
        emitEvents();
        detectAndReportHiccup();
        if (logger.isFineEnabled()) {
            reportThroughput();
        }
        return false;
    }

    private void emitEvents() {
        while (emitFromTraverser(traverser) && emitSchedule <= nowNanos) {
            long timestamp = NANOSECONDS.toMillis(emitSchedule) - nanoTimeMillisToCurrentTimeMillis;
            traverser.append(jetEvent(timestamp, counter * totalParallelism + globalProcessorIndex));
            counter++;
            emitSchedule += emitPeriod;
            if (timestamp >= lastEmittedWm + wmGranularity) {
                long wmToEmit = timestamp - (timestamp % wmGranularity) + wmOffset;
                traverser.append(new Watermark(wmToEmit));
                lastEmittedWm = wmToEmit;
            }
        }
    }

    private void detectAndReportHiccup() {
        long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
        if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
            logger.info(String.format("*** Source #%d hiccup: %,d ms%n", globalProcessorIndex, millisSinceLastCall));
        }
        lastCallNanos = nowNanos;
    }

    private void reportThroughput() {
        long nanosSinceLastReport = nowNanos - lastReport;
        if (nanosSinceLastReport < REPORT_PERIOD_NANOS) {
            return;
        }
        lastReport = nowNanos;
        long itemCountSinceLastReport = counter - counterAtLastReport;
        counterAtLastReport = counter;
        logger.fine(String.format("p%d: %,.0f items/second%n",
                globalProcessorIndex,
                itemCountSinceLastReport / ((double) nanosSinceLastReport / SECONDS.toNanos(1))));
    }

    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }

}
