/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Easy-to-use sources intended to be used during pipeline development.
 * It's often convenient to have easy-to-use sources with well defined
 * behaviour when developing a streaming application.
 *
 * @since 3.1
 */
public final class DevSources {
    private static final AtomicLong COUNTER = new AtomicLong();
    private static final String NAME_PREFIX = "dev-source-";

    private DevSources() {

    }

    /**
     * Streaming source which emits local wall-clock timestamps with specified
     * minimum delay between items. It attempts to compensate for various
     * system hiccups so the rate over a period of time is constant.
     * <p>
     * Each item is a local wall-clock timestamp. The source is not distributed
     * it means only a single Jet instance will emit items.
     *
     * @param period   period which to emit
     * @param timeUnit units for period
     * @return source emitting with a fixed rate
     */
    @Nonnull
    public static StreamSource<Long> fixedRate(long period, @Nonnull TimeUnit timeUnit) {
        return fixedRate(period, timeUnit, System::currentTimeMillis);
    }

    /**
     * Streaming source which emits local wall-clock timestamps with specified
     * minimum delay between items.
     * <p>
     * It won't emit unless elapsed time since last emit is at least the
     * specified delay. There is no upper bound on the delay at that's partially
     * driven by the Jet engine. In practice it will behave similar to
     * fixedDelay.
     * <p>
     * The source is not distributed it means only a single Jet instance
     * will emit items.
     *
     * @param delay minimum delay between emitting
     * @param timeUnit units of the delay
     * @return source emitting with a specified minimum delay
     */
    @Nonnull
    public static StreamSource<? extends Long> minimumDelay(long delay, @Nonnull TimeUnit timeUnit) {
        return minimumDelay(delay, timeUnit, System::currentTimeMillis);
    }

    /**
     * Streaming source which emit items at fixed rate. It attempts to compensate
     * for various system hiccups so the rate over a period of time is constant.
     * <p>
     * You have to provide your own item supplier.
     * The source is not distributed it means only a single Jet instance will
     * emit items.
     * <p>
     * @param period period which to emit
     * @param timeUnit units for period
     * @param itemSupplier supplier of items to be emitted. It's called
     *                     whenever a new item is about to be emitted
     * @param <T> type of emitted items
     * @return source emitting with a fixed rate
     */
    @Nonnull
    public static <T> StreamSource<T> fixedRate(
            long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> itemSupplier) {
        return triggerDrivenStreamSource(() -> new FixedRateTrigger(period, timeUnit), itemSupplier);
    }

    /**
     * Streaming source which emits with specified minimum delay between items.
     * <p>
     * It won't emit unless elapsed time since last emit is at least the
     * specified delay. There is no upper bound on the delay at that's
     * partially driven by the Jet engine. In practice it will behave similar
     * to fixedDelay.
     * <p>
     * You have to provide your own item supplier.
     * The source is not distributed it means only a single Jet instance will emit items.
     *
     * @param delay delay minimum delay between emitting
     * @param timeUnit units of the delay
     * @param itemSupplier supplier of items to be emitted. It's called
     *                     whenever a new item is about to be emitted
     * @param <T> type of emitted items
     * @return source emitting with a specified minimum delay
     */
    @Nonnull
    public static <T> StreamSource<T> minimumDelay(
            long delay, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> itemSupplier) {
        return triggerDrivenStreamSource(() -> new MinDelayTrigger(delay, timeUnit), itemSupplier);
    }

    /**
     * Batch source producing specified elements.
     *
     * @param items values to emit
     * @param <T> type of the element
     * @return source emitting all specified items
     */
    @Nonnull
    public static <T> BatchSource<T> of(@Nonnull T...items) {
        return fromIterator(() -> Arrays.asList(items).iterator());
    }

    @Nonnull
    private static <T> StreamSource<T> triggerDrivenStreamSource(
            @Nonnull SupplierEx<? extends Trigger> triggerSupplier, @Nonnull SupplierEx<? extends T> itemSupplier) {
        return SourceBuilder.stream(newName(), c -> triggerSupplier.get())
                .<T>fillBufferFn((trigger, buffer) -> {
                    while (trigger.shouldEmit()) {
                        buffer.add(itemSupplier.get());
                    }
                }).build();
    }

    @Nonnull
    private static <T> BatchSource<T> fromIterator(@Nonnull SupplierEx<Iterator<? extends T>> iteratorSupplier) {
        return SourceBuilder.batch(newName(), c -> iteratorSupplier.get())
                .<T>fillBufferFn((i, b) -> {
                    if (i.hasNext()) {
                        T item = i.next();
                        b.add(item);
                    } else {
                        b.close();
                    }
                }).build();
    }

    @Nonnull
    private static String newName() {
        return NAME_PREFIX + COUNTER.getAndIncrement();
    }

    private interface Trigger {
        boolean shouldEmit();
    }

    private static final class MinDelayTrigger implements Trigger {
        private long lastEmit;
        private final long delayNanos;

        private MinDelayTrigger(long period, @Nonnull TimeUnit timeUnit) {
            this.lastEmit = System.nanoTime();
            this.delayNanos = timeUnit.toNanos(period);
        }

        @Override
        public boolean shouldEmit() {
            long now = System.nanoTime();
            if (now > lastEmit + delayNanos) {
                lastEmit = now;
                return true;
            }
            return false;
        }
    }

    private static final class FixedRateTrigger implements Trigger {
        private long lastEmit;
        private final long periodNanos;

        private FixedRateTrigger(long period, @Nonnull TimeUnit timeUnit) {
            this.lastEmit = System.nanoTime();
            this.periodNanos = timeUnit.toNanos(period);
        }

        @Override
        public boolean shouldEmit() {
            long now = System.nanoTime();
            if (now > lastEmit + periodNanos) {
                lastEmit += periodNanos;
                return true;
            }
            return false;
        }
    }
}
