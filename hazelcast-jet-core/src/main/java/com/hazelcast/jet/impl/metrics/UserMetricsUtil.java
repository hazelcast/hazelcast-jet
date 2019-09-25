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
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;

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

    public static <T, U> BiPredicateEx<T, U> wrap(BiPredicateEx<T, U> biPredicateEx, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedBiPredicateEx<>(biPredicateEx,
                    Collections.singletonList((ProvidesMetrics) metricsProviderCandidate));
        } else {
            return biPredicateEx;
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

    public static <T, U, R> BiFunctionEx<T, U, R> wrap(BiFunctionEx<T, U, R> biFunctionEx,
                                                       Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedBiFunctionEx<>(biFunctionEx,
                    Collections.singletonList((ProvidesMetrics) metricsProviderCandidate));
        } else {
            return biFunctionEx;
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

    public static <T0, T1, T2, R> TriFunction<T0, T1, T2, R> wrap(TriFunction<T0, T1, T2, R> triFunction,
                                                                  Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedTriFunction<>(triFunction,
                    Collections.singletonList((ProvidesMetrics) metricsProviderCandidate));
        } else {
            return triFunction;
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
    private static class WrappedAggregateOperation1 extends AbstractWrapper implements AggregateOperation1 {
        private final AggregateOperation1 aggrOp1;

        WrappedAggregateOperation1(AggregateOperation1 aggrOp1, List<ProvidesMetrics> providers) {
            super(providers);
            this.aggrOp1 = aggrOp1;
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
    }

    @SuppressWarnings("unchecked")
    private static class WrappedAggregateOperation2 extends AbstractWrapper implements AggregateOperation2 {
        private final AggregateOperation2 aggrOp2;

        WrappedAggregateOperation2(AggregateOperation2 aggrOp2, List<ProvidesMetrics> providers) {
            super(providers);
            this.aggrOp2 = aggrOp2;
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
    }

    private static class WrappedBiPredicateEx<T, U> extends AbstractWrapper implements BiPredicateEx<T, U> {
        private final BiPredicateEx<T, U> predicateEx;

        WrappedBiPredicateEx(BiPredicateEx<T, U> predicateEx, List<ProvidesMetrics> providers) {
            super(providers);
            this.predicateEx = predicateEx;
        }

        @Override
        public boolean testEx(T t, U u) throws Exception {
            return predicateEx.testEx(t, u);
        }
    }

    private static class WrappedFunctionEx<T, R> extends AbstractWrapper implements FunctionEx<T, R> {
        private final FunctionEx<T, R> functionEx;

        WrappedFunctionEx(FunctionEx<T, R> functionEx, List<ProvidesMetrics> providers) {
            super(providers);
            this.functionEx = functionEx;
        }

        @Override
        public R applyEx(T t) throws Exception {
            return functionEx.applyEx(t);
        }
    }

    private static class WrappedBiFunctionEx<T, U, R> extends AbstractWrapper implements BiFunctionEx<T, U, R> {
        private final BiFunctionEx<T, U, R> biFunctionEx;

        WrappedBiFunctionEx(BiFunctionEx<T, U, R> biFunctionEx, List<ProvidesMetrics> providers) {
            super(providers);
            this.biFunctionEx = biFunctionEx;
        }

        @Override
        public R applyEx(T t, U u) throws Exception {
            return biFunctionEx.applyEx(t, u);
        }
    }

    private static class WrappedSupplierEx<P> extends AbstractWrapper implements SupplierEx<P> {
        private final SupplierEx<P> supplierEx;

        WrappedSupplierEx(SupplierEx<P> supplierEx, List<ProvidesMetrics> providers) {
            super(providers);
            this.supplierEx = supplierEx;
        }

        @Override
        public P getEx() throws Exception {
            return supplierEx.getEx();
        }
    }

    private static class WrappedTriFunction<T0, T1, T2, R> extends AbstractWrapper implements TriFunction<T0, T1, T2, R> {
        private final TriFunction<T0, T1, T2, R> triFunction;

        WrappedTriFunction(TriFunction<T0, T1, T2, R> triFunction, List<ProvidesMetrics> providers) {
            super(providers);
            this.triFunction = triFunction;
        }

        @Override
        public R applyEx(T0 t0, T1 t1, T2 t2) throws Exception {
            return triFunction.applyEx(t0, t1, t2);
        }
    }

    private abstract static class AbstractWrapper implements ProvidesMetrics, Serializable {
        private final List<ProvidesMetrics> providers;

        AbstractWrapper(List<ProvidesMetrics> providers) {
            this.providers = providers;
        }

        @Override
        public void registerMetrics(MetricsContext context) {
            for (ProvidesMetrics provider : providers) {
                provider.registerMetrics(context);
            }
        }
    }

}
