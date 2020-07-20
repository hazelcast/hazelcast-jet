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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents an {@link AbstractProcessor} that indefinitely emits {@code
 * long} values. The purpose of the class is to enable creating a
 * distributed {@link com.hazelcast.jet.pipeline.StreamSource} for
 * high-throughput performance testing. An example usage can be found in
 * {@link TestSources#streamSourceLong(long itemsPerSecond, long
 * initialDelay, int preferredLocalParallelism)}.
 *
 * @since 4.3
 */
public class StreamSourceLong extends AbstractProcessor {

    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS = 10;

    private static final long REPORT_PERIOD_NANOS = SECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS);
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;
    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long startTime;
    private final long itemsPerSecond;
    private final ILogger logger = Logger.getLogger(StreamSourceLong.class);
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

    /**
     * Creates a stream source for {@code long} values.
     *
     * @param startTime when to start in milliseconds
     * @param itemsPerSecond how many items, i.e., {@code long} values, should be emitted each second
     * @param eventTimePolicy which {@linkplain EventTimePolicy} to apply
     *
     * @since 4.3
     */
    @Nonnull
    public StreamSourceLong(
            long startTime,
            long itemsPerSecond,
            EventTimePolicy<? super Long> eventTimePolicy
    ) {
        this.wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
        this.wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
        this.startTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
        this.itemsPerSecond = itemsPerSecond;
    }

    /**
     * Initializes this stream source by setting various attributes, such as
     * {@link #totalParallelism}, {@link #globalProcessorIndex}, and the {@link
     * #emitPeriod}.
     *
     * @param context processor context
     *
     * @since 4.3
     */
    @Override
    protected void init(Context context) {
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        emitPeriod = SECONDS.toNanos(1) * totalParallelism / itemsPerSecond;
        lastCallNanos = lastReport = emitSchedule =
                startTime + SECONDS.toNanos(1) * globalProcessorIndex / itemsPerSecond;
    }

    /**
     * Invokes the emission of events ({@link #emitEvents()}), the detection
     * and reporting of hiccups ({@link #detectAndReportHiccup()}), and reports
     * the throughput ({@link #reportThroughput()}) if the log level is set to
     * {@link java.util.logging.Level#FINE}. Always returns false, i.e., never
     * indicates that the source is complete to emit events for a potentially
     * infinite time, which is typical for stream source processors (see {@link
     * Processor#complete()}).
     *
     * @return always {@code false} so that this method is called again
     *
     * @since 4.3
     */
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

    /**
     * Emits stream events ({@link com.hazelcast.jet.impl.JetEvent}) according
     * to the defined rate/schedule. The events contain a {@code long} value
     * that represents the product of the event counter, {@link
     * ProcessorMetaSupplier.Context#totalParallelism()}, and {@link
     * Context#globalProcessorIndex()}. An event is emitted by appending it to
     * the {@link #traverser}. According the the defined watermark granularity
     * ({@link #wmGranularity}), watermarks are also appended to the {@link
     * AppendableTraverser}.
     *
     * @since 4.3
     */
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

    /**
     * Detects hiccups and reports them to the log along with an information
     * about the global processor index (see {@link Context#globalProcessorIndex()}.
     *
     * @since 4.3
     */
    private void detectAndReportHiccup() {
        long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
        if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
            logger.info(String.format("*** Source #%d hiccup: %,d ms%n", globalProcessorIndex, millisSinceLastCall));
        }
        lastCallNanos = nowNanos;
    }

    /**
     * Reports the achieved throughput to the log in defined reporting
     * intervals as number of items/second, including an information about the
     * global processor index (see {@link Context#globalProcessorIndex()}).
     *
     * @since 4.3
     */
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

    /**
     * Calculates the difference between {@link System#nanoTime()} converted to
     * milliseconds and {@link System#currentTimeMillis()}.
     *
     * @return the calculated time offset/difference
     *
     * @since 4.3
     */
    private static long determineTimeOffset() {
        return NANOSECONDS.toMillis(System.nanoTime()) - System.currentTimeMillis();
    }

}
