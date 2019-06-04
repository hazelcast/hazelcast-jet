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

package com.hazelcast.jet.function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Factory methods for commons suppliers.
 *
 * @since 3.1
 */
public final class Suppliers {

    private Suppliers() {

    }

    /**
     * Wrap the passed supplier with another supplier which will throttle
     * maximum rate of item supplying.
     * <p>
     * Normally the wrapper just delegates to the passed supplier. When it's
     * called more frequently than the specified rate then the wrapper will
     * return <code>null</code> instead of calling the delegate.
     * <p>
     * The throttling rate is calculated since the first invocation
     * of {@link Supplier#get()}. When you do not call it for a period
     * of a time then the throttling wrapper gives you extra burst budget to
     * make up for it. In other words: It temporarily allows to generate items
     * with a higher rate until the average rate reaches the specified rate.
     * <p>
     * If this burst behaviour is not what you want then
     * {@see #minDelayThrottle}
     *
     * @param period throttling period
     * @param timeUnit units for the throttling period
     * @param delegate supplier to delegate to
     * @param <T> type of items provided by the supplier
     * @return throttling supplier
     */
    @Nonnull
    public static <T> SupplierEx<T> fixedRateThrottle(
            long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> delegate
    ) {
        return new Suppliers.FixedRateThrottle<>(period, timeUnit, delegate);
    }

    /**
     * Wrap the passed supplier with another supplier which will enforce
     * minimal delay between 2 consecutive item generation.
     * <p>
     * Normally the wrapper just delegates to the passed supplier. When it's
     * called more frequently than the specified minimal delay then the wrapper
     * will return <code>null</code> instead of calling the delegate.
     * <p>
     * Unlike {@link #fixedRateThrottle(long, TimeUnit, SupplierEx)} it does
     * not give you extra budget after a quiesce period.
     *
     * @param delay minimal delay between invocations
     * @param timeUnit units for the delay
     * @param delegate supplier to delegate to
     * @param <T> type of items provided by the supplier
     * @return throttling supplier
     */
    @Nonnull
    public static <T> SupplierEx<T> minDelayThrottle(
            long delay, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> delegate
    ) {
        return new MinDelayThrottle<>(delay, timeUnit, delegate);
    }

    private static final class FixedRateThrottle<T> implements SupplierEx<T> {
        private static final long serialVersionUID = 1L;
        private final SupplierEx<? extends T> delegate;
        private transient long lastEmit;
        private final long periodNanos;

        FixedRateThrottle(long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> delegate) {
            this.periodNanos = timeUnit.toNanos(period);
            this.delegate = delegate;
        }

        @Nullable @Override
        public T getEx() throws Exception {
            long now = System.nanoTime();
            if (lastEmit == 0) {
                // first call -> we want to emit.
                lastEmit = now - periodNanos;
            }
            if (now >= lastEmit + periodNanos) {
                T item = delegate.getEx();
                if (item == null) {
                    return null;
                }
                lastEmit += periodNanos;
                return item;
            }
            return null;
        }
    }

    private static final class MinDelayThrottle<T> implements SupplierEx<T> {
        private static final long serialVersionUID = 1L;
        private final SupplierEx<? extends T> delegate;
        private transient long lastEmit;
        private final long delayNanos;

        MinDelayThrottle(long delay, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> delegate) {
            this.delayNanos = timeUnit.toNanos(delay);
            this.delegate = delegate;
        }

        @Nullable @Override
        public T getEx() throws Exception {
            long now = System.nanoTime();
            if (lastEmit == 0) {
                // first call -> we want to emit.
                lastEmit = now - delayNanos;
            }

            if (now > lastEmit + delayNanos) {
                T item = delegate.getEx();
                if (item == null) {
                    return null;
                }
                lastEmit = now;
                return item;
            }
            return null;
        }
    }
}
