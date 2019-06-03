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

final class SuppliersEx {
    final static class FixedRateThrottle<T> implements SupplierEx<T> {
        private final SupplierEx<? extends T> delegate;
        private transient long lastEmit;
        private final long periodNanos;

        FixedRateThrottle(long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> delegate) {
            this.periodNanos = timeUnit.toNanos(period);
            this.delegate = delegate;
        }

        @Override
        @Nullable
        public T getEx() throws Exception {
            long now = System.nanoTime();
            if (lastEmit == 0) {
                // first call -> we want to emit.
                lastEmit = System.nanoTime() - lastEmit;
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

    final static class MinDelayThrottle<T> implements SupplierEx<T> {
        private final SupplierEx<? extends T> delegate;
        private transient long lastEmit;
        private final long delayNanos;

        MinDelayThrottle(long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> delegate) {
            this.delayNanos = timeUnit.toNanos(period);
            this.delegate = delegate;
        }

        @Override
        @Nullable
        public T getEx() throws Exception {
            long now = System.nanoTime();
            if (lastEmit == 0) {
                // first call -> we want to emit.
                lastEmit = System.nanoTime() - lastEmit;
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
