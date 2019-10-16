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

import com.hazelcast.jet.impl.metrics.UserMetricsImpl;

/**
 * Utility class for obtaining handler to user-defined metrics.
 * <p>
 * User-defined metric are simple numeric values used to count or
 * measure things, just like the built-in ones, the difference being
 * that they can be set up by users to measure and count thing
 * they do in pipeline user code they write.
 * <p>
 * This class provides methods for creating various types of handlers
 * for setting up and manipulating the values of such metrics. There is
 * a version which makes it possible to count things (see {@link Counter}),
 * or one that is more generic and can be used to set all types of values,
 * including counters (see {@link Gauge}).
 */
@SuppressWarnings("WeakerAccess")
public final class UserMetrics {

    private UserMetrics() {
    }

    /**
     * Returns a counter type handler for manipulating the metric with
     * the specified name. Is equivalent with a gauge of the unit
     * {@link Unit#COUNT}.
     */
    public static Counter getCounter(String name) {
        return UserMetricsImpl.getCounter(name);
    }

    /**
     * Returns a gauge type handler for manipulating the metric with the
     * specified name.
     */
    public static Gauge getGauge(String name, Unit unit) {
        return UserMetricsImpl.getGauge(name, unit);
    }

}
