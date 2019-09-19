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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.core.metrics.MetricsContext;
import com.hazelcast.jet.core.metrics.ProvidesMetrics;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public final class UserMetricsUtil {

    private static final ProvidesMetrics DUMMY_METRICS_PROVIDER = context -> { };

    private UserMetricsUtil() {
    }

    public static ProvidesMetrics cast(Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return (ProvidesMetrics) metricsProviderCandidate;
        } else {
            return DUMMY_METRICS_PROVIDER;
        }
    }

    public static <T, R> FunctionEx<T, R> wrap(FunctionEx<T, R> functionEx, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedFunctionEx<>(functionEx,
                    Collections.singletonList((ProvidesMetrics) metricsProviderCandidate));
        } else {
            return functionEx;
        }
    }

    public static <T, R> FunctionEx<T, R> wrapAll(FunctionEx<T, R> functionEx, List<?> metricsProviderCandidates) {
        List<ProvidesMetrics> providers = metricsProviderCandidates.stream()
                .filter(ProvidesMetrics.class::isInstance)
                .map(ProvidesMetrics.class::cast)
                .collect(toList());
        if (providers.isEmpty()) {
            return functionEx;
        } else {
            return new WrappedFunctionEx<>(functionEx, providers);
        }
    }

    public static <P> SupplierEx<P> wrap(SupplierEx<P> supplierEx, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedSupplierEx<>(supplierEx,
                    Collections.singletonList((ProvidesMetrics) metricsProviderCandidate));
        } else {
            return supplierEx;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends AggregateOperation> T wrap(T aggrOp, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            List<ProvidesMetrics> providers = Collections.singletonList((ProvidesMetrics) metricsProviderCandidate);
            if (aggrOp.arity() == 1) {
                return (T) new WrappedAggregateOperation1((AggregateOperation1) aggrOp, providers);
            } else {
                return (T) new WrappedAggregateOperation2((AggregateOperation2) aggrOp, providers);
            }
        }
        return aggrOp;
    }

    @SuppressWarnings("unchecked")
    public static <T extends AggregateOperation> T wrapAll(T aggrOp, List<?> metricsProviderCandidates) {
        List<ProvidesMetrics> providers = metricsProviderCandidates.stream()
                .filter(ProvidesMetrics.class::isInstance)
                .map(ProvidesMetrics.class::cast)
                .collect(toList());
        if (!providers.isEmpty()) {
            if (aggrOp.arity() == 1) {
                return (T) new WrappedAggregateOperation1((AggregateOperation1) aggrOp, providers);
            } else if (aggrOp.arity() == 2) {
                return (T) new WrappedAggregateOperation2((AggregateOperation2) aggrOp, providers);
            }
        }
        return aggrOp;
    }

    @SuppressWarnings("unchecked")
    private static class WrappedAggregateOperation1 implements AggregateOperation1, ProvidesMetrics, Serializable {

        private final AggregateOperation1 aggrOp1;
        private final List<ProvidesMetrics> providers;


        WrappedAggregateOperation1(AggregateOperation1 aggrOp1, List<ProvidesMetrics> providers) {
            this.aggrOp1 = aggrOp1;
            this.providers = providers;
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn() {
            return aggrOp1.accumulateFn();
        }

        @Nonnull
        @Override
        public AggregateOperation1 withAccumulateFn(BiConsumerEx accumulateFn) {
            return aggrOp1.withAccumulateFn(accumulateFn);
        }

        @Nonnull
        @Override
        public AggregateOperation1 withIdentityFinish() {
            return aggrOp1.withIdentityFinish();
        }

        @Nonnull
        @Override
        public AggregateOperation1 andThen(FunctionEx thenFn) {
            return aggrOp1.andThen(thenFn);
        }

        @Override
        public int arity() {
            return aggrOp1.arity();
        }

        @Nonnull
        @Override
        public SupplierEx createFn() {
            return aggrOp1.createFn();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn(int index) {
            return aggrOp1.accumulateFn(index);
        }

        @Nullable
        @Override
        public BiConsumerEx combineFn() {
            return aggrOp1.combineFn();
        }

        @Nullable
        @Override
        public BiConsumerEx deductFn() {
            return aggrOp1.deductFn();
        }

        @Nonnull
        @Override
        public FunctionEx exportFn() {
            return aggrOp1.exportFn();
        }

        @Nonnull
        @Override
        public FunctionEx finishFn() {
            return aggrOp1.finishFn();
        }

        @Nonnull
        @Override
        public AggregateOperation withAccumulateFns(BiConsumerEx... accumulateFns) {
            return aggrOp1.withAccumulateFns(accumulateFns);
        }

        @Override
        public void init(MetricsContext context) {
            for (ProvidesMetrics provider : providers) {
                provider.init(context);
            }
        }
    }



    @SuppressWarnings("unchecked")
    private static class WrappedAggregateOperation2 implements AggregateOperation2, ProvidesMetrics, Serializable {

        private final AggregateOperation2 aggrOp2;
        private final List<ProvidesMetrics> providers;


        WrappedAggregateOperation2(AggregateOperation2 aggrOp2, List<ProvidesMetrics> providers) {
            this.aggrOp2 = aggrOp2;
            this.providers = providers;
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn0() {
            return aggrOp2.accumulateFn0();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn1() {
            return aggrOp2.accumulateFn1();
        }

        @Nonnull
        @Override
        public AggregateOperation2 withAccumulateFn0(@Nonnull BiConsumerEx accumulateFn) {
            return aggrOp2.withAccumulateFn0(accumulateFn);
        }

        @Nonnull
        @Override
        public AggregateOperation2 withAccumulateFn1(@Nonnull BiConsumerEx accumulateFn) {
            return aggrOp2.withAccumulateFn1(accumulateFn);
        }

        @Nonnull
        @Override
        public AggregateOperation2 withIdentityFinish() {
            return aggrOp2.withIdentityFinish();
        }

        @Nonnull
        @Override
        public AggregateOperation2 andThen(FunctionEx thenFn) {
            return aggrOp2.andThen(thenFn);
        }

        @Override
        public int arity() {
            return aggrOp2.arity();
        }

        @Nonnull
        @Override
        public SupplierEx createFn() {
            return aggrOp2.createFn();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn(int index) {
            return aggrOp2.accumulateFn(index);
        }

        @Nullable
        @Override
        public BiConsumerEx combineFn() {
            return aggrOp2.combineFn();
        }

        @Nullable
        @Override
        public BiConsumerEx deductFn() {
            return aggrOp2.deductFn();
        }

        @Nonnull
        @Override
        public FunctionEx exportFn() {
            return aggrOp2.exportFn();
        }

        @Nonnull
        @Override
        public FunctionEx finishFn() {
            return aggrOp2.finishFn();
        }

        @Nonnull
        @Override
        public AggregateOperation withAccumulateFns(BiConsumerEx... accumulateFns) {
            return aggrOp2.withAccumulateFns(accumulateFns);
        }

        @Override
        public void init(MetricsContext context) {
            for (ProvidesMetrics provider : providers) {
                provider.init(context);
            }
        }
    }

    private static class WrappedFunctionEx<T, R> implements FunctionEx<T, R>, ProvidesMetrics, Serializable {

        private final FunctionEx<T, R> functionEx;
        private final List<ProvidesMetrics> providers;

        WrappedFunctionEx(FunctionEx<T, R> functionEx, List<ProvidesMetrics> providers) {
            this.functionEx = functionEx;
            this.providers = providers;
        }

        @Override
        public R applyEx(T t) throws Exception {
            return functionEx.applyEx(t);
        }

        @Override
        public void init(MetricsContext context) {
            for (ProvidesMetrics provider : providers) {
                provider.init(context);
            }
        }
    }

    private static class WrappedSupplierEx<P> implements SupplierEx<P>, ProvidesMetrics, Serializable {

        private final SupplierEx<P> supplierEx;
        private final List<ProvidesMetrics> providers;

        WrappedSupplierEx(SupplierEx<P> supplierEx, List<ProvidesMetrics> providers) {
            this.supplierEx = supplierEx;
            this.providers = providers;
        }

        @Override
        public P getEx() throws Exception {
            return supplierEx.getEx();
        }

        @Override
        public void init(MetricsContext context) {
            for (ProvidesMetrics provider : providers) {
                provider.init(context);
            }
        }
    }

}
