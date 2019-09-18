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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.jet.core.metrics.MetricsContext;
import com.hazelcast.jet.core.metrics.MetricsOperator;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public final class MetricsOperatorUtil {

    private static final MetricsOperator DUMMY_OP = new MetricsOperator() {
        @Override
        public void init(MetricsContext context) {
        }
    };

    private MetricsOperatorUtil() {
    }

    public static MetricsOperator cast(Object operatorCandidate) {
        if (operatorCandidate instanceof MetricsOperator) {
            return (MetricsOperator) operatorCandidate;
        } else {
            return DUMMY_OP;
        }
    }

    public static <T, R> FunctionEx<T, R> wrap(FunctionEx<T, R> functionEx, Object operatorCandidate) {
        if (operatorCandidate instanceof MetricsOperator) {
            return new WrappedFunctionEx<>(functionEx, Collections.singletonList((MetricsOperator) operatorCandidate));
        } else {
            return functionEx;
        }
    }

    public static <T, R> FunctionEx<T, R> wrapAll(FunctionEx<T, R> functionEx, List<?> operatorCandidates) {
        List<MetricsOperator> operators = operatorCandidates.stream()
                .filter(MetricsOperator.class::isInstance)
                .map(MetricsOperator.class::cast)
                .collect(toList());
        if (operators.isEmpty()) {
            return functionEx;
        } else {
            return new WrappedFunctionEx<>(functionEx, operators);
        }
    }

    public static <P> SupplierEx<P> wrap(SupplierEx<P> supplierEx, Object operatorCandidate) {
        if (operatorCandidate instanceof MetricsOperator) {
            return new WrappedSupplierEx<>(supplierEx, Collections.singletonList((MetricsOperator) operatorCandidate));
        } else {
            return supplierEx;
        }
    }

    private static class WrappedFunctionEx<T, R> implements FunctionEx<T, R>, MetricsOperator, Serializable {

        private final FunctionEx<T, R> functionEx;
        private final List<MetricsOperator> metricsOperators;

        WrappedFunctionEx(FunctionEx<T, R> functionEx, List<MetricsOperator> metricsOperators) {
            this.functionEx = functionEx;
            this.metricsOperators = metricsOperators;
        }

        @Override
        public R applyEx(T t) throws Exception {
            return functionEx.applyEx(t);
        }

        @Override
        public void init(MetricsContext context) {
            for (MetricsOperator operator : metricsOperators) {
                operator.init(context);
            }
        }
    }

    private static class WrappedSupplierEx<P> implements SupplierEx<P>, MetricsOperator, Serializable {

        private final SupplierEx<P> supplierEx;
        private final List<MetricsOperator> metricsOperators;

        WrappedSupplierEx(SupplierEx<P> supplierEx, List<MetricsOperator> metricsOperators) {
            this.supplierEx = supplierEx;
            this.metricsOperators = metricsOperators;
        }

        @Override
        public P getEx() throws Exception {
            return supplierEx.getEx();
        }

        @Override
        public void init(MetricsContext context) {
            for (MetricsOperator operator : metricsOperators) {
                operator.init(context);
            }
        }
    }

}
