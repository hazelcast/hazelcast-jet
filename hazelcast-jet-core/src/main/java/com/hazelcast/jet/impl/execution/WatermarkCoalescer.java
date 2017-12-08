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
 * when to forward the WM:<ul>
 *     <li>WM is forwarded, after it is received from all queues
 *     <li>or earlier, based on maximum watermark retention
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
     * Call when queue with this index is exhausted.
     *
     * @return Watermark timestamp to emit or {@code Long.MIN_VALUE}
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
     * Call this method after receiving a WM.
     *
     * @param realTime Current system time
     * @param queueIndex Index of queue on which the WM was received.
     * @param wmValue Watermark timestamp
     *
     * @return Watermark timestamp to emit or {@code Long.MIN_VALUE}
     */
    public long observeWm(long realTime, int queueIndex, long wmValue) {
        if (queueWms[queueIndex] >= wmValue) {
            throw new JetException("Watermarks not monotonically increasing on queue: " +
                    "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
        }
        queueWms[queueIndex] = wmValue;

        long wmToEmit = Long.MIN_VALUE;

        if (watermarkHistory != null && wmValue > topObservedWm) {
            topObservedWm = wmValue;
            wmToEmit = watermarkHistory.sample(realTime, topObservedWm);
        }

        wmToEmit = Math.max(wmToEmit, bottomObservedWm());
        if (wmToEmit > lastEmittedWm) {
            lastEmittedWm = wmToEmit;
            return wmToEmit;
        }

        return Long.MIN_VALUE;
    }

    /**
     * Checks if there is watermark to emit now based on real time passage.
     *
     * @param realTime Current system time
     * @return Watermark timestamp to emit or {@code Long.MIN_VALUE}
     */
    public long checkWmHistory(long realTime) {
        if (watermarkHistory == null) {
            return Long.MIN_VALUE;
        }
        long historicWm = watermarkHistory.sample(realTime, topObservedWm);
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
