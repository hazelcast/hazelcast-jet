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
import java.util.Objects;

/**
 * Lazy version of {@link ThrowingSupplier}, initializes the value it's
 * going to supply once, on first usage, caches it, then returns it on
 * every subsequent {@code get}.
 * <p>
 * <b>NOT</b> thread safe.
 *
 * @param <T> type of supplied values
 * @param <E> type of thrown exception
 *
 * @since 4.1
 */
public class LazyThrowingSupplier<T, E extends Exception> implements ThrowingSupplier<T, E> {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final ThrowingSupplier<T, E> expensiveSupplier;

    private transient T value;

    /**
     * Gets initialized with any {@link ThrowingSupplier} that will be
     * used at most once, to create the cached value.
     */
    public LazyThrowingSupplier(@Nonnull ThrowingSupplier<T, E> expensiveSupplier) {
        this.expensiveSupplier = Objects.requireNonNull(expensiveSupplier);
    }

    /**
     * Returns the cached result, creating it if needed, potentially
     * throwing an {@code E extends Exception} during the process.
     */
    public T get() throws E {
        if (value == null) {
            value = expensiveSupplier.get();
        }
        return value;
    }
}
