/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.function.ObjLongBiFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class WatermarkSourceUtilImpl<T> implements WatermarkSourceUtil<T> {

    private static final WatermarkPolicy[] EMPTY_WATERMARK_POLICIES = {};
    private static final long[] EMPTY_LONGS = {};

    private final long idleTimeoutNanos;
    @Nullable
    private final ToLongFunction<? super T> timestampFn;
    private final Supplier<? extends WatermarkPolicy> newWmPolicyFn;
    private final ObjLongBiFunction<? super T, ?> wrapFn;
    private final SlidingWindowPolicy watermarkThrottlingFrame;
    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);

    private WatermarkPolicy[] wmPolicies = EMPTY_WATERMARK_POLICIES;
    private long[] watermarks = EMPTY_LONGS;
    private long[] markIdleAt = EMPTY_LONGS;
    private long lastEmittedWm = Long.MIN_VALUE;
    private long topObservedWm = Long.MIN_VALUE;
    private boolean allAreIdle;

    public WatermarkSourceUtilImpl(EventTimePolicy<? super T> eventTimePolicy) {
        checkPositive(eventTimePolicy.watermarkThrottlingFrameSize(), "watermarkThrottlingFrameSize must be >= 1");
        this.idleTimeoutNanos = MILLISECONDS.toNanos(eventTimePolicy.idleTimeoutMillis());
        this.timestampFn = eventTimePolicy.timestampFn();
        this.wrapFn = eventTimePolicy.wrapFn();
        this.newWmPolicyFn = eventTimePolicy.newWmPolicyFn();
        this.watermarkThrottlingFrame = tumblingWinPolicy(eventTimePolicy.watermarkThrottlingFrameSize())
                .withOffset(eventTimePolicy.watermarkThrottlingFrameOffset());
    }

    @Nonnull
    @Override
    public Traverser<Object> handleEvent(T event, int partitionIndex, long nativeEventTime) {
        return handleEvent(System.nanoTime(), event, partitionIndex, nativeEventTime);
    }

    @Nonnull @Override
    public Traverser<Object> handleNoEvent() {
        return handleEvent(System.nanoTime(), null, -1, NO_NATIVE_TIME);
    }

    // package-visible for tests
    Traverser<Object> handleEvent(long now, @Nullable T event, int partitionIndex, long nativeEventTime) {
        assert traverser.isEmpty() : "the traverser returned previously not yet drained: remove all " +
                "items from the traverser before you call this method again.";
        if (event != null) {
            if (timestampFn == null && nativeEventTime == NO_NATIVE_TIME) {
                throw new JetException("Neither timestampFn nor nativeEventTime specified");
            }
            long eventTime = timestampFn == null ? nativeEventTime : timestampFn.applyAsLong(event);
            handleEventInt(now, partitionIndex, eventTime);
            traverser.append(wrapFn.apply(event, eventTime));
        } else {
            handleNoEventInt(now);
        }
        return traverser;
    }

    private void handleEventInt(long now, int partitionIndex, long eventTime) {
        wmPolicies[partitionIndex].reportEvent(eventTime);
        markIdleAt[partitionIndex] = now + idleTimeoutNanos;
        allAreIdle = false;
        handleNoEventInt(now);
    }

    private void handleNoEventInt(long now) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < watermarks.length; i++) {
            if (idleTimeoutNanos > 0 && markIdleAt[i] <= now) {
                continue;
            }
            watermarks[i] = wmPolicies[i].getCurrentWatermark();
            topObservedWm = Math.max(topObservedWm, watermarks[i]);
            min = Math.min(min, watermarks[i]);
        }

        if (min == Long.MAX_VALUE) {
            if (allAreIdle) {
                return;
            }
            // we've just became fully idle. Forward the top WM now, if needed
            min = topObservedWm;
            allAreIdle = true;
        } else {
            allAreIdle = false;
        }

        if (min > lastEmittedWm) {
            long newWm = watermarkThrottlingFrame.floorFrameTs(min);
            if (newWm > lastEmittedWm) {
                traverser.append(new Watermark(newWm));
                lastEmittedWm = newWm;
            }
        }
        if (allAreIdle) {
            traverser.append(IDLE_MESSAGE);
        }
    }

    @Override
    public void increasePartitionCount(int newPartitionCount) {
        increasePartitionCount(System.nanoTime(), newPartitionCount);
    }

    // package-visible for tests
    void increasePartitionCount(long now, int newPartitionCount) {
        int oldPartitionCount = wmPolicies.length;
        if (newPartitionCount < oldPartitionCount) {
            throw new IllegalArgumentException("partition count must increase. Old count=" + oldPartitionCount
                    + ", new count=" + newPartitionCount);
        }

        wmPolicies = Arrays.copyOf(wmPolicies, newPartitionCount);
        watermarks = Arrays.copyOf(watermarks, newPartitionCount);
        markIdleAt = Arrays.copyOf(markIdleAt, newPartitionCount);

        for (int i = oldPartitionCount; i < newPartitionCount; i++) {
            wmPolicies[i] = newWmPolicyFn.get();
            watermarks[i] = Long.MIN_VALUE;
            markIdleAt[i] = now + idleTimeoutNanos;
        }
    }

    @Override
    public long getWatermark(int partitionIndex) {
        return watermarks[partitionIndex];
    }

    @Override
    public void restoreWatermark(int partitionIndex, long wm) {
        watermarks[partitionIndex] = wm;
    }
}
