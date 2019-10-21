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
 * Handler for manipulating one user-defined metric.
 * <p>
 * Metrics are, in essence just simple integer values, with various semantics.
 * They can be used for example for counting things (like number of certain
 * events) or for storing standalone values (like measurements). The various
 * methods of this interface are meant to facilitate all these use-cases.
 * <p>
 * To obtain an instance, use {@link Metrics#metric}.
 */
public interface Metric {

    /**
     * Returns the name of the associated metric.
     */
    @Nonnull
    String name();

    /**
     * Return the unit of measurement of the associated metric. Meant
     * to provide further information on the type of value measured
     * by the user-metric. Doesn't affect the functionality of the metric,
     * it still remains a simple numeric value, but is used to
     * populate the {@link MetricTags#UNIT} tag in the metric's description.
     */
    Unit unit();

    /**
     * Increments the current value by 1.
     */
    void inc();

    /**
     * Increments the current value by the specified amount.
     */
    void add(long amount);

    /**
     * Decrements the current value by 1.
     */
    void dec();

    /**
     * Decrements the current value by the specified amount.
     */
    void sub(long amount);

    /**
     * Sets the current value.
     */
    void set(long newValue);

    /**
     * Returns the current value.
     */
    long get();

}
