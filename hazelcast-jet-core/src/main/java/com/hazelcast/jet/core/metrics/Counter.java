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

import javax.annotation.Nonnull;

/**
 * A handler to manipulate one user-defined metric. To obtain an instance, use
 * {@link UserMetrics#get}.
 * <p>
 * Implementation is not thread-safe, the methods must be called only from the
 * processor thread.
 */
public interface Counter {

    /**
     * Returns the name of the associated metric.
     */
    @Nonnull
    String name();

    /**
     * Sets the metric to the specified value.
     */
    void set(long newValue);

    /**
     * Increments the current value by 1.
     */
    void increment();

    /**
     * Increments the current value by the specified amount.
     */
    void add(long amount);

    /**
     * Decrements the current value by 1.
     */
    void decrement();

    /**
     * Decrements the current value by the specified amount.
     */
    void subtract(long amount);

    /**
     * Returns the current value of the metric.
     */
    long get();
}
