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

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * --- Useful info goes here! --- //todo
 */
public interface UserMetricsSource {

    /**
     * --- Useful info goes here! --- //todo
     */
    @Nonnull
    List<Object> getMetricsSources();

    /**
     * --- Useful info goes here! --- //todo
     */
    static <T, R> FunctionEx<T, R> wrap(FunctionEx<T, R> functionEx, List<Object> userMetricsSources) {
        return new WrappedFunctionEx<>(functionEx, userMetricsSources);
    }

    /**
     * --- Useful info goes here! --- //todo
     */
    static <P> SupplierEx<P> wrap(SupplierEx<P> supplierEx, List<Object> userMetricsSources) {
        return new WrappedSupplierEx<>(supplierEx, userMetricsSources);
    }

    /**
     * --- Useful info goes here! --- //todo
     * @param <T>
     * @param <R>
     */
    class WrappedFunctionEx<T, R> implements FunctionEx<T, R>, UserMetricsSource, Serializable {

        private final FunctionEx<T, R> functionEx;
        private final List<Object> metricsSources;

        WrappedFunctionEx(@Nonnull FunctionEx<T, R> functionEx, @Nonnull List<Object> metricsSources) {
            Objects.requireNonNull(functionEx, "functionEx");
            Objects.requireNonNull(metricsSources, "metricsSources");

            this.functionEx = functionEx;
            this.metricsSources = metricsSources;
        }

        @Override
        public R applyEx(T t) throws Exception {
            return functionEx.applyEx(t);
        }

        @Nonnull
        @Override
        public List<Object> getMetricsSources() {
            return metricsSources;
        }
    }

    /**
     * --- Useful info goes here! --- //todo
     * @param <P>
     */
    class WrappedSupplierEx<P> implements SupplierEx<P>, UserMetricsSource, Serializable {

        private final SupplierEx<P> supplierEx;
        private final List<Object> metricsSources;

        WrappedSupplierEx(SupplierEx<P> supplierEx, List<Object> metricsSources) {
            Objects.requireNonNull(supplierEx, "supplierEx");
            Objects.requireNonNull(metricsSources, "metricsSources");

            this.supplierEx = supplierEx;
            this.metricsSources = metricsSources;
        }

        @Override
        public P getEx() throws Exception {
            return supplierEx.getEx();
        }

        @Nonnull
        @Override
        public List<Object> getMetricsSources() {
            return metricsSources;
        }
    }

}
