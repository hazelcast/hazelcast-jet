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

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements {@link Watermark} coalescing. Tracks WMs on queues and decides
 * when to forward the WM. The watermark should be forwarded:
 * <ul>
 *     <li>when it has been received from all input streams (ignoring idle streams)
 *     <li>if the maximum watermark retention time has elapsed
 * </ul>
 *
 * The class also handles idle messages from inputs (coming in the form of a
 * watermark with {@link #IDLE_QUEUE_WATERMARK_VALUE} timestamp. When such
 * message is received, that input is switched to <em>idle</em> and excluded
 * from coalescing. Any event or watermark from such input will turn the input
 * back to <em>active</em> state.
 *
 * TODO move to public package?
 */
public abstract class WatermarkCoalescer {

    static long IDLE_QUEUE_WATERMARK_VALUE = Long.MAX_VALUE;

    private WatermarkCoalescer() { }

    /**
     * Called when the queue with the given index is exhausted.
     *
     * @return the watermark value to emit or {@code Long.MIN_VALUE} if no watermark
     * should be forwarded
     */
    public abstract long queueDone(int queueIndex);

    /**
     * Called after receiving a new event. Will change the queue state to
     * <em>active</em>.
     *
     * @param queueIndex index of the queue on which the event was received.
     */
    public abstract void observeEvent(int queueIndex);

    /**
     * Called after receiving a new watermark.
     *
     * @param queueIndex index of the queue on which the WM was received.
     * @param wmValue    the watermark value, it can be {@link #IDLE_QUEUE_WATERMARK_VALUE}
     * @return the watermark value to emit or {@code Long.MIN_VALUE} if no watermark
     * should be forwarded
     */
    public long observeWm(int queueIndex, long wmValue) {
        return observeWm(System.nanoTime(), queueIndex, wmValue);
    }

    // package-visible for testing
    abstract long observeWm(long systemTime, int queueIndex, long wmValue);

    /**
     * Checks if there is a watermark to emit now based on the passage of
     * system time or if all input queues are idle and we should forward the
     * idle marker.
     *
     * @return the watermark value to emit, {@link #IDLE_QUEUE_WATERMARK_VALUE}
     * or {@code Long.MIN_VALUE} if no watermark should be forwarded
     */
    public long checkWmHistory() {
        return checkWmHistory(System.nanoTime());
    }

    // package-visible for testing
    abstract long checkWmHistory(long systemTime);

    /**
     * Returns {@code System.nanoTime()} or a dummy value, if it is not needed,
     * because the call is expensive in hot loop.
     */
    abstract long getTime();

    /**
     * Factory method.
     *
     * @param maxWatermarkRetainMillis see {@link com.hazelcast.jet.config.JobConfig#setMaxWatermarkRetainMillis}
     * @param queueCount number of queues
     */
    public static WatermarkCoalescer create(int maxWatermarkRetainMillis, int queueCount) {
        checkNotNegative(queueCount, "queueCount must be >= 0, but is " + queueCount);
        switch (queueCount) {
            case 0:
                return new ZeroInputImpl();
            default:
                return new StandardImpl(maxWatermarkRetainMillis, queueCount);
        }
    }

    /**
     * Special-case implementation for zero inputs.
     */
    private static final class ZeroInputImpl extends WatermarkCoalescer {

        @Override
        public void observeEvent(int queueIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long observeWm(long systemTime, int queueIndex, long wmValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long queueDone(int queueIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long checkWmHistory(long systemTime) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getTime() {
            return -1;
        }
    }

    /**
     * Standard implementation for 1..n inputs.
     */
    private static final class StandardImpl extends WatermarkCoalescer {

        private final TimestampHistory watermarkHistory;
        private final long[] queueWms;
        private final boolean[] isIdle;
        private long lastEmittedWm = Long.MIN_VALUE;
        private long topObservedWm = Long.MIN_VALUE;
        private int numActiveQueues;

        StandardImpl(int maxWatermarkRetainMillis, int queueCount) {
            numActiveQueues = queueCount;
            isIdle = new boolean[queueCount];
            queueWms = new long[queueCount];
            Arrays.fill(queueWms, Long.MIN_VALUE);

            watermarkHistory = maxWatermarkRetainMillis >= 0 && queueCount > 1
                    ? new TimestampHistory(MILLISECONDS.toNanos(maxWatermarkRetainMillis))
                    : null;
        }

        @Override
        public long queueDone(int queueIndex) {
            queueWms[queueIndex] = Long.MAX_VALUE;

            long bottomVm = bottomObservedWm();
            if (bottomVm > lastEmittedWm && bottomVm != Long.MAX_VALUE) {
                lastEmittedWm = bottomVm;
                return bottomVm;
            }

            return Long.MIN_VALUE;
        }

        @Override
        public void observeEvent(int queueIndex) {
            if (isIdle[queueIndex]) {
                isIdle[queueIndex] = false;
                numActiveQueues++;
            }
        }

        @Override
        public long observeWm(long systemTime, int queueIndex, long wmValue) {
            if (queueWms[queueIndex] >= wmValue) {
                throw new JetException("Watermarks not monotonically increasing on queue: " +
                        "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
            }

            if (wmValue == IDLE_QUEUE_WATERMARK_VALUE) {
                if (isIdle[queueIndex]) {
                    throw new JetException("Duplicate IDLE message");
                }
                isIdle[queueIndex] = true;
                numActiveQueues--;
                if (numActiveQueues == 0) {
                    // all inputs are idle now, let's forward the message
                    return IDLE_QUEUE_WATERMARK_VALUE;
                }
            } else {
                if (isIdle[queueIndex]) {
                    isIdle[queueIndex] = false;
                    numActiveQueues++;
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
            }

            return Long.MIN_VALUE;
        }

        @Override
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

        @Override
        public long getTime() {
            return watermarkHistory != null ? System.nanoTime() : -1;
        }

        private long bottomObservedWm() {
            assert numActiveQueues > 0;
            long min = Long.MAX_VALUE;
            for (int i = 0; i < queueWms.length; i++) {
                if (!isIdle[i] && queueWms[i] < min) {
                    min = queueWms[i];
                }
            }
            return min;
        }
    }
}
