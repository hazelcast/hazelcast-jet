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

import com.hazelcast.function.SupplierEx;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Lazy version of {@link SupplierEx}, initializes the value it's going
 * to supply once, on first usage, caches it, then returns it on every
 * subsequent {@code get}.
 * <p>
 * <b>NOT</b> thread safe.
 *
 * @param <T> type of value that will be supplied
 *
 * @since 4.1
 */
public class LazySupplier<T> implements SupplierEx<T> {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final SupplierEx<T> expensiveSupplier;

    private transient T value;

    /**
     * Gets initialized with any {@link SupplierEx} that will be used at
     * most once, to create the cached value.
     */
    public LazySupplier(@Nonnull SupplierEx<T> expensiveSupplier) {
        this.expensiveSupplier = Objects.requireNonNull(expensiveSupplier);
    }

    /**
     * Returns the cached result, creating it if needed.
     */
    @Override
    public T getEx() {
        if (value == null) {
            value = expensiveSupplier.get();
        }
        return value;
    }
}
