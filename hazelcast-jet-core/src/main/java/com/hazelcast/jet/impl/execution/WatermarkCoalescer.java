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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.TimestampHistory;

import java.util.Arrays;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements {@link Watermark} coalescing. Tracks WMs on queues and decides
 * when to forward the WM. The watermark should be forwarded:
 * <ul>
 *     <li>when it has been received from all input streams
 *     <li>if the maximum watermark retention time has elapsed
 * </ul>
 */
class WatermarkCoalescer {

    private final long[] queueWms;
    private final TimestampHistory watermarkHistory;

    private long lastEmittedWm = Long.MIN_VALUE;
    private long topObservedWm = Long.MIN_VALUE;

    WatermarkCoalescer(int maxWatermarkRetainMillis, int queueCount) {
        queueWms = new long[queueCount];
        Arrays.fill(queueWms, Long.MIN_VALUE);

        watermarkHistory = maxWatermarkRetainMillis >= 0
                ? new TimestampHistory(MILLISECONDS.toNanos(maxWatermarkRetainMillis))
                : null;
    }

    /**
     * Called when the queue with the given index is exhausted.
     *
     * @return the watermark value to emit or {@code Long.MIN_VALUE} if no watermark
     *         should be emitted
     */
    public long queueDone(int queueIndex) {
        queueWms[queueIndex] = Long.MAX_VALUE;

        long bottomVm = bottomObservedWm();
        if (bottomVm > lastEmittedWm && bottomVm != Long.MAX_VALUE) {
            lastEmittedWm = bottomVm;
            return bottomVm;
        }

        return Long.MIN_VALUE;
    }

    /**
     * Called after receiving a new watermark.
     *
     * @param systemTime current system time
     * @param queueIndex index of queue on which the WM was received.
     * @param wmValue the watermark value
     *
     * @return the watermark value to emit or {@code Long.MIN_VALUE}
     */
    public long observeWm(long systemTime, int queueIndex, long wmValue) {
        if (queueWms[queueIndex] >= wmValue) {
            throw new JetException("Watermarks not monotonically increasing on queue: " +
                    "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
        }
        queueWms[queueIndex] = wmValue;

        long wmToEmit = Long.MIN_VALUE;

        if (watermarkHistory != null && wmValue > topObservedWm) {
            topObservedWm = wmValue;
            wmToEmit = watermarkHistory.sample(systemTime, topObservedWm);
        }

        wmToEmit = Math.max(wmToEmit, bottomObservedWm());
        if (wmToEmit > lastEmittedWm) {
            lastEmittedWm = wmToEmit;
            return wmToEmit;
        }

        return Long.MIN_VALUE;
    }

    /**
     * Checks if there is a watermark to emit now based on the passage of
     * system time.
     *
     * @param systemTime Current system time
     * @return Watermark timestamp to emit or {@code Long.MIN_VALUE}
     */
    public long checkWmHistory(long systemTime) {
        if (watermarkHistory == null) {
            return Long.MIN_VALUE;
        }
        long historicWm = watermarkHistory.sample(systemTime, topObservedWm);
        if (historicWm > lastEmittedWm) {
            lastEmittedWm = historicWm;
            return historicWm;
        }
        return Long.MIN_VALUE;
    }

    public boolean usesWmHistory() {
        return watermarkHistory != null;
    }

    private long bottomObservedWm() {
        long min = queueWms[0];
        for (int i = 1; i < queueWms.length; i++) {
            if (queueWms[i] < min) {
                min = queueWms[i];
            }
        }
        return min;
    }
}
