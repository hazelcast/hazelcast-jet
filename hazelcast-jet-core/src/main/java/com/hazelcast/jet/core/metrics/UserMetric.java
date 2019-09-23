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
 * Can be used to explicitly set the value of one particular
 * user-defined metric.
 *
 * Implementation are thread-safe, calling all methods is safe to be done
 * from any thread.
 */
public interface UserMetric {

    /**
     * Returns the name of the metric being manipulated.
     */
    @Nonnull
    String getName();

    /**
     * Sets the metric to the specified value.
     */
    void updateValue(long newValue);

    /**
     * Increments the current value with 1.
     */
    void incValue();

    /**
     * Increments the current value with the specified amount.
     */
    void incValue(long increment);

    /**
     * Decrements the current value with 1.
     */
    void decValue();

    /**
     * Decrements the current value with the specified amount.
     */
    void decValue(long decrement);
}
