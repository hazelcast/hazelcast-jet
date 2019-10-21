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

import com.hazelcast.jet.impl.metrics.MetricsImpl;

/**
 * Utility class for obtaining handler to user-defined metrics.
 * <p>
 * User-defined metric are simple numeric values used to count or
 * measure things, just like the built-in ones, the difference being
 * that they can be set up by users to measure and count thing
 * they do in pipeline user code they write.
 * <p>
 * This class provides provides the means for creating of handlers
 * for setting up and manipulating the values of such metrics.
 */
@SuppressWarnings("WeakerAccess")
public final class Metrics {

    private Metrics() {
    }

    /**
     * Returns a handler for manipulating the metric with the specified name.
     */
    public static Metric metric(String name) {
        return MetricsImpl.metric(name, Unit.COUNT, false);
    }

    /**
     * Returns a handler for manipulating the metric with the
     * specified name and measurement unit.
     */
    public static Metric metric(String name, Unit unit) {
        return MetricsImpl.metric(name, unit, false);
    }

    /**
     * Returns a handler for manipulating the metric with the
     * specified name and measurement unit.
     * <p>
     * Also has a parameter which can be used to specify the threading
     * behaviour of the returned metric. Normally this is no concern. The
     * default metric type is non-thread-safe, the user-code gets executed
     * in such a way that it will still function properly. However it is
     * possible to write user code in such a way that it will get executed
     * on "foreign" (ie. not Jet internal threads) and in those situations
     * we must set this threadSafe parameter to true. See following example
     * code:
     * <pre>
     * {@code
     *  p.drawFrom(..)
     *   .filterUsingContextAsync(
     *       ContextFactory.withCreateFn(i -> 0L),
     *       (ctx, l) -> {
     *           Metric dropped = Metrics.metric("dropped", Unit.COUNT, true);
     *           return CompletableFuture.supplyAsync(
     *               () -> {
     *                   boolean pass = l % 2L == ctx;
     *                   if (!pass) {
     *                       dropped.inc();
     *                   }
     *                   return pass;
     *               }
     *           );
     *       }
     *   )
     *   .drainTo(...);
     * }
     * </pre>
     *
     *
     */
    public static Metric metric(String name, Unit unit, boolean threadSafe) {
        return MetricsImpl.metric(name, unit, threadSafe);
    }

}
