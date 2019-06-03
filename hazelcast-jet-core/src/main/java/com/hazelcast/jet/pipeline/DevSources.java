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

import static com.hazelcast.jet.function.SupplierEx.fixedRateThrottle;
import static com.hazelcast.jet.function.SupplierEx.minDelayThrottle;

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
        return streamSource(() -> fixedRateThrottle(period, timeUnit, itemSupplier));
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
        return streamSource(() -> minDelayThrottle(delay, timeUnit, itemSupplier));
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
        return fromSuppliedIterator(() -> Arrays.asList(items).iterator());
    }

    /**
     * TODO
     *
     * @param iteratorSupplier
     * @param <T>
     * @return
     */
    @Nonnull
    public static <T> BatchSource<T> fromSuppliedIterator(@Nonnull SupplierEx<Iterator<? extends T>> iteratorSupplier) {
        return batchSource(() -> {
            Iterator<? extends T> iterator = iteratorSupplier.get();
            return () -> {
                if (!iterator.hasNext()) {
                    return null;
                } else {
                    return iterator.next();
                }
            };
        });

    }

    /**
     * TODO
     * Semantic: the inner supplier returns null to indicate no item is available at this time
     *
     * @param supplierOfSuppliers
     * @param <T>
     * @return
     */
    @Nonnull
    private static <T> StreamSource<T> streamSource(@Nonnull SupplierEx<SupplierEx<? extends T>> supplierOfSuppliers) {
        return SourceBuilder.stream(newName(), c -> supplierOfSuppliers.get())
                .<T>fillBufferFn((p, b) -> {
                    T t = p.get();
                    while (t != null) {
                        b.add(t);
                        t = p.get();
                    }
                }).build();
    }

    /**
     * TODO
     * Semantic: the inner supplier returns null to indicate there are no more items available in the batch
     *
     *
     * @param supplierOfSuppliers
     * @param <T>
     * @return
     */
    @Nonnull
    private static <T> BatchSource<T> batchSource(@Nonnull SupplierEx<SupplierEx<? extends T>> supplierOfSuppliers) {
        return SourceBuilder.batch(newName(), c -> supplierOfSuppliers.get())
                .<T>fillBufferFn((p, b) -> {
                    T t = p.get();
                    if (t != null) {
                        b.add(t);
                    } else {
                        b.close();
                    }
                }).build();
    }

    @Nonnull
    private static String newName() {
        return NAME_PREFIX + COUNTER.getAndIncrement();
    }
}
