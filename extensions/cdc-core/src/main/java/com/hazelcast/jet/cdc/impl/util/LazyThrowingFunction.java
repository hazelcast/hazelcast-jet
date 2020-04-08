/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.impl.util;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Lazy version of {@link ThrowingFunction}, computes the result for any
 * particular input only once, on first call, caches it, then
 * returns it on every subsequent call with the same input.
 * <p>
 * Result cache never gets cleaned, so memory consumption can get out of
 * hand, if used improperly.
 * <p>
 * <b>NOT</b> thread safe.
 *
 * @param <T> type of function parameter
 * @param <R> type of function result
 * @param <E> type of thrown exception
 *
 * @since 4.1
 */
public class LazyThrowingFunction<T, R, E extends Exception> implements ThrowingFunction<T, R, E> {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final ThrowingFunction<T, R, E> expensiveFunction;

    private transient Map<T, R> values;

    /**
     * Gets initialized with any {@link ThrowingFunction} that will be
     * used at most once for any input, to create the cached result for
     * that input.
     */
    public LazyThrowingFunction(@Nonnull ThrowingFunction<T, R, E> expensiveFunction) {
        this.expensiveFunction = Objects.requireNonNull(expensiveFunction);
    }

    /**
     * Returns the cached result for a particular input, creating it if
     * needed, potentially throwing an {@code E extends Exception}
     * during the process.
     */
    @Override
    public R apply(T t) throws E {
        if (values == null) {
            values = new HashMap<>();
        }

        R value = values.get(t);
        if (value == null) {
            value = expensiveFunction.apply(t);
            values.put(t, value);
        }
        return value;
    }
}
