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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;

/**
 * Context object which is able to provide localised counter instances to
 * be used as sources of custom metrics. Each {@link com.hazelcast.jet.core.Processor}
 * will have its own instance. Hence user metrics have the same granularity
 * as Processors.
 */
public interface MetricsContext {

    /**
     * Return a {@link Counter} object that can be used to set values
     * for one specific user-defined, per Processor metric, identified
     * by the provided name.
     * <p>
     * Calling this method for metrics which have already had an implicit
     * supplier set is not allowed and will result in an {@code IllegalStateException}
     * being thrown.
     */
    @Nonnull
    Counter registerCounter(String name);

    /**
     * Specifies an implicit value supplier for a user-defined, per Processor
     * metric, identified by the provided name.
     * <p>
     * Calling this method for metrics for which a {@link Counter} handler
     * has already been retrieved is not allowed and will result in an
     * {@code IllegalStateException} being thrown.
     * <p>
     * Setting a gauge after it has already been set will also result in
     * an {@code IllegalStateException} being thrown.
     * <p>
     * Care needs to be taken that the supplier passed in as a parameter is
     * thread-safe. For example:
     *
     * <pre>{@code
     *     AtomicLong counter = new AtomicLong();
     *     ...
     *     context.registerGauge(counter::get);
     *     ...
     *     counter.incrementAndGet();
     *     counter.incrementAndGet();
     * }</pre>
     */
    void registerGauge(@Nonnull String name, @Nonnull SupplierEx<Long> supplier);
}
