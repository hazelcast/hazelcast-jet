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
 * A measurement-like handler for manipulating one user-defined metric.
 * <p>
 * Metrics are, in essence just simple integer values, but their semantics
 * can differ from case-to-case. This representation should be used for
 * metrics that are meant to represent things that get measured.
 * <p>
 * To obtain an instance, use {@link UserMetrics#getGauge}.
 */
public interface Gauge {

    /**
     * Returns the name of the associated metric.
     */
    @Nonnull
    String name();

    /**
     * Sets the metric to the specified value.
     */
    void set(long newValue);
}
