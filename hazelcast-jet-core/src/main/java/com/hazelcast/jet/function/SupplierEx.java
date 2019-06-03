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

package com.hazelcast.jet.function;

import com.hazelcast.jet.impl.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * {@code Serializable} variant of {@link Supplier java.util.function.Supplier}
 * which declares checked exception.
 *
 * @since 3.0
 */
@FunctionalInterface
public interface SupplierEx<T> extends Supplier<T>, Serializable {

    /**
     * Exception-declaring version of {@link Supplier#get}.
     */
    T getEx() throws Exception;

    @Override
    default T get() {
        try {
            return getEx();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * JavaDoc
     *
     * @param period
     * @param timeUnit
     * @param supplierEx
     * @param <T>
     * @return
     */
    @Nonnull
    static <T> SupplierEx<T> fixedRateThrottle(
            long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> supplierEx) {
        return new SuppliersEx.FixedRateThrottle<>(period, timeUnit, supplierEx);
    }

    /**
     * JavaDoc
     *
     * @param period
     * @param timeUnit
     * @param supplierEx
     * @param <T>
     * @return
     */
    @Nonnull
    static <T> SupplierEx<T> minDelayThrottle(
            long period, @Nonnull TimeUnit timeUnit, @Nonnull SupplierEx<? extends T> supplierEx) {
        return new SuppliersEx.MinDelayThrottle<>(period, timeUnit, supplierEx);
    }
}
