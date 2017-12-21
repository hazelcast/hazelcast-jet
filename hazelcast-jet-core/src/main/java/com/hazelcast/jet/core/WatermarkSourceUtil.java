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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A utility to help emitting {@link Watermark} from source.
 *
 * @param <T> event type
 */
public class WatermarkSourceUtil<T> {

    private final long idleTimeoutNanos;
    private final DistributedToLongFunction<T> getTimestampF;
    private final DistributedSupplier<WatermarkPolicy> newWmPolicyF;
    private final WatermarkEmissionPolicy wmEmitPolicy;

    private WatermarkPolicy[] wmPolicies = {};
    private long[] watermarks = {};
    private long[] markIdleAt = {};
    private long lastEmittedWm = Long.MIN_VALUE;
    private boolean allAreIdle;

    private AppendableTraverser<Object> flatMapTraverser = new AppendableTraverser<>(2);

    public WatermarkSourceUtil(int initialPartitionCount, long idleTimeoutMillis,
                               @Nonnull DistributedToLongFunction<T> getTimestampF,
                               @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
                               @Nonnull WatermarkEmissionPolicy wmEmitPolicy) {
        this(System.nanoTime(), initialPartitionCount, idleTimeoutMillis, getTimestampF, newWmPolicyF, wmEmitPolicy);
    }

    // package-visible for tests
    WatermarkSourceUtil(long systemTime, int initialPartitionCount, long idleTimeoutMillis,
                        @Nonnull DistributedToLongFunction<T> getTimestampF,
                        @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
                        @Nonnull WatermarkEmissionPolicy wmEmitPolicy) {
        this.idleTimeoutNanos = MILLISECONDS.toNanos(idleTimeoutMillis);
        this.getTimestampF = getTimestampF;
        this.newWmPolicyF = newWmPolicyF;
        this.wmEmitPolicy = wmEmitPolicy;

        increasePartitionCount(systemTime, initialPartitionCount);
    }

    /**
     * Called after the event was emitted to decide if a watermark should be
     * sent after it.
     *
     * @param event the event
     * @param partitionIndex index of the partition the event occurred in
     *
     * @return watermark to emit or {@code null}
     */
    public Watermark observeEvent(T event, int partitionIndex) {
        return observeEvent(System.nanoTime(), event, partitionIndex);
    }

    // package-visible for tests
    Watermark observeEvent(long now, T event, int partitionIndex) {
        long eventTime = getTimestampF.applyAsLong(event);
        watermarks[partitionIndex] = wmPolicies[partitionIndex].reportEvent(eventTime);
        markIdleAt[partitionIndex] = now + idleTimeoutNanos;
        return handleNoEvent(now);
    }

    /**
     * Call when there are no observed events. Checks, if a watermark should be
     * emitted based on the passage of time, which could cause some partitions
     * to become idle.
     *
     * @return watermark to emit or {@code null}
     */
    public Watermark handleNoEvent() {
        return handleNoEvent(System.nanoTime());
    }

    // package-visible for tests
    Watermark handleNoEvent(long now) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < watermarks.length; i++) {
            if (idleTimeoutNanos > 0 && markIdleAt[i] <= now) {
                continue;
            }
            watermarks[i] = wmPolicies[i].getCurrentWatermark();
            min = Math.min(min, watermarks[i]);
        }

        if (min == Long.MAX_VALUE) {
            if (allAreIdle) {
                return null;
            }
            allAreIdle = true;
            return IDLE_MESSAGE;
        }

        if (!wmEmitPolicy.shouldEmit(min, lastEmittedWm)) {
            return null;
        }
        allAreIdle = false;
        lastEmittedWm = min;
        return new Watermark(min);
    }

    /**
     * Changes the partition count. New partitions will be marked as ACTIVE.
     *
     * @param newPartitionCount partition count, must be higher than the
     *                          current count
     */
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
            wmPolicies[i] = newWmPolicyF.get();
            watermarks[i] = Long.MIN_VALUE;
            markIdleAt[i] = now + idleTimeoutNanos;
        }
    }

    public Traverser<Object> flatMapEvent(T event, int partitionIndex) {
        return flatMapEvent(System.nanoTime(), event, partitionIndex);
    }

    // package-visible for tests
    Traverser<Object> flatMapEvent(long now, T event, int partitionIndex) {
        assert flatMapTraverser.isEmpty() : "Traverser wasn't empty";

        flatMapTraverser.append(event);
        Watermark wm = observeEvent(now, event, partitionIndex);
        if (wm != null) {
            flatMapTraverser.append(wm);
        }
        return flatMapTraverser;
    }
}
