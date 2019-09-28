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
import com.hazelcast.jet.aggregate.AggregateOperation3;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

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
            return new WrappedBiPredicateEx<>(biPredicateEx, (ProvidesMetrics) metricsProviderCandidate);
        } else {
            return biPredicateEx;
        }
    }

    public static <T, U> BiConsumerEx<T, U> wrap(BiConsumerEx<T, U> biConsumerEx, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedBiConsumerEx<>(biConsumerEx, (ProvidesMetrics) metricsProviderCandidate);
        } else {
            return biConsumerEx;
        }
    }

    public static <T, U> BiConsumerEx<T, U> wrapAll(BiConsumerEx<T, U> biConsumerEx, List<?> metricsProviderCandidates) {
        Set<ProvidesMetrics> providers = getProvidesMetrics(metricsProviderCandidates);
        if (providers.isEmpty()) {
            return biConsumerEx;
        } else {
            return new WrappedBiConsumerEx<>(biConsumerEx, providers);
        }
    }

    public static <T, R> FunctionEx<T, R> wrap(FunctionEx<T, R> functionEx, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedFunctionEx<>(functionEx, (ProvidesMetrics) metricsProviderCandidate);
        } else {
            return functionEx;
        }
    }

    public static <T, R> FunctionEx<T, R> wrapAll(FunctionEx<T, R> functionEx, List<?> metricsProviderCandidates) {
        Set<ProvidesMetrics> providers = getProvidesMetrics(metricsProviderCandidates);
        if (providers.isEmpty()) {
            return functionEx;
        } else {
            return new WrappedFunctionEx<>(functionEx, providers);
        }
    }

    public static <T, U, R> BiFunctionEx<T, U, R> wrap(BiFunctionEx<T, U, R> biFunctionEx,
                                                       Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedBiFunctionEx<>(biFunctionEx, (ProvidesMetrics) metricsProviderCandidate);
        } else {
            return biFunctionEx;
        }
    }

    public static <T0, T1, T2, R> TriFunction<T0, T1, T2, R> wrap(TriFunction<T0, T1, T2, R> triFunction,
                                                                  Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedTriFunction<>(triFunction, (ProvidesMetrics) metricsProviderCandidate);
        } else {
            return triFunction;
        }
    }

    public static <P> SupplierEx<P> wrap(SupplierEx<P> supplierEx, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            return new WrappedSupplierEx<>(supplierEx, (ProvidesMetrics) metricsProviderCandidate);
        } else {
            return supplierEx;
        }
    }

    public static <P> SupplierEx<P> wrapAll(SupplierEx<P> supplierEx, List<?> metricsProviderCandidates) {
        Set<ProvidesMetrics> providers = getProvidesMetrics(metricsProviderCandidates);
        if (providers.isEmpty()) {
            return supplierEx;
        } else {
            return new WrappedSupplierEx<>(supplierEx, providers);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends AggregateOperation> T wrap(T aggrOp, Object metricsProviderCandidate) {
        if (metricsProviderCandidate instanceof ProvidesMetrics) {
            if (aggrOp.arity() == 1) {
                return (T) new WrappedAggregateOperation1((AggregateOperation1) aggrOp,
                        (ProvidesMetrics) metricsProviderCandidate);
            } else if (aggrOp.arity() == 2) {
                return (T) new WrappedAggregateOperation2((AggregateOperation2) aggrOp,
                        (ProvidesMetrics) metricsProviderCandidate);
            } else if (aggrOp.arity() == 3) {
                return (T) new WrappedAggregateOperation3((AggregateOperation3) aggrOp,
                        (ProvidesMetrics) metricsProviderCandidate);
            }
        }
        return aggrOp;
    }

    @SuppressWarnings("unchecked")
    public static <T extends AggregateOperation> T wrapAll(T aggrOp, List<?> metricsProviderCandidates) {
        Set<ProvidesMetrics> providers = getProvidesMetrics(metricsProviderCandidates);
        if (!providers.isEmpty()) {
            if (aggrOp.arity() == 1) {
                return (T) new WrappedAggregateOperation1((AggregateOperation1) aggrOp, providers);
            } else if (aggrOp.arity() == 2) {
                return (T) new WrappedAggregateOperation2((AggregateOperation2) aggrOp, providers);
            } else if (aggrOp.arity() == 3) {
                return (T) new WrappedAggregateOperation3((AggregateOperation3) aggrOp, providers);
            }
        }
        return aggrOp;
    }

    private static Set<ProvidesMetrics> getProvidesMetrics(List<?> metricsProviderCandidates) {
        return metricsProviderCandidates.stream()
                .filter(ProvidesMetrics.class::isInstance)
                .map(ProvidesMetrics.class::cast)
                .collect(toSet());
    }

    @SuppressWarnings("unchecked")
    private static class WrappedAggregateOperation1 extends AbstractWrapper implements AggregateOperation1 {
        private final AggregateOperation1 aggrOp1;

        WrappedAggregateOperation1(AggregateOperation1 aggrOp1, ProvidesMetrics provider) {
            super(provider);
            this.aggrOp1 = aggrOp1;
        }

        WrappedAggregateOperation1(AggregateOperation1 aggrOp1, Collection<ProvidesMetrics> providers) {
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

        WrappedAggregateOperation2(AggregateOperation2 aggrOp2, ProvidesMetrics provider) {
            super(provider);
            this.aggrOp2 = aggrOp2;
        }

        WrappedAggregateOperation2(AggregateOperation2 aggrOp2, Collection<ProvidesMetrics> providers) {
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

    @SuppressWarnings("unchecked")
    private static class WrappedAggregateOperation3 extends AbstractWrapper implements AggregateOperation3 {
        private final AggregateOperation3 aggrOp3;

        WrappedAggregateOperation3(AggregateOperation3 aggrOp3, ProvidesMetrics provider) {
            super(provider);
            this.aggrOp3 = aggrOp3;
        }

        WrappedAggregateOperation3(AggregateOperation3 aggrOp3, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.aggrOp3 = aggrOp3;
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn0() {
            return aggrOp3.accumulateFn0();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn1() {
            return aggrOp3.accumulateFn1();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn2() {
            return aggrOp3.accumulateFn2();
        }

        @Nonnull
        @Override
        public AggregateOperation3 withAccumulateFn0(@Nonnull BiConsumerEx accumulateFn) {
            return aggrOp3.withAccumulateFn0(accumulateFn);
        }

        @Nonnull
        @Override
        public AggregateOperation3 withAccumulateFn1(@Nonnull BiConsumerEx accumulateFn) {
            return aggrOp3.withAccumulateFn1(accumulateFn);
        }

        @Nonnull
        @Override
        public AggregateOperation3 withAccumulateFn2(@Nonnull BiConsumerEx accumulateFn) {
            return aggrOp3.withAccumulateFn2(accumulateFn);
        }

        @Nonnull
        @Override
        public AggregateOperation3 withIdentityFinish() {
            return aggrOp3.withIdentityFinish();
        }

        @Nonnull
        @Override
        public AggregateOperation3 andThen(FunctionEx thenFn) {
            return aggrOp3.andThen(thenFn);
        }

        @Override
        public int arity() {
            return aggrOp3.arity();
        }

        @Nonnull
        @Override
        public SupplierEx createFn() {
            return aggrOp3.createFn();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn(int index) {
            return aggrOp3.accumulateFn(index);
        }

        @Nullable
        @Override
        public BiConsumerEx combineFn() {
            return aggrOp3.combineFn();
        }

        @Nullable
        @Override
        public BiConsumerEx deductFn() {
            return aggrOp3.deductFn();
        }

        @Nonnull
        @Override
        public FunctionEx exportFn() {
            return aggrOp3.exportFn();
        }

        @Nonnull
        @Override
        public FunctionEx finishFn() {
            return aggrOp3.finishFn();
        }

        @Nonnull
        @Override
        public AggregateOperation withAccumulateFns(BiConsumerEx... accumulateFns) {
            return aggrOp3.withAccumulateFns(accumulateFns);
        }
    }

    private static class WrappedBiPredicateEx<T, U> extends AbstractWrapper implements BiPredicateEx<T, U> {
        private final BiPredicateEx<T, U> predicateEx;

        WrappedBiPredicateEx(BiPredicateEx<T, U> predicateEx, ProvidesMetrics provider) {
            super(provider);
            this.predicateEx = predicateEx;
        }

        @Override
        public boolean testEx(T t, U u) throws Exception {
            return predicateEx.testEx(t, u);
        }
    }

    private static class WrappedBiConsumerEx<T, U> extends AbstractWrapper implements BiConsumerEx<T, U> {
        private final BiConsumerEx<T, U> consumerEx;

        WrappedBiConsumerEx(BiConsumerEx<T, U> consumerEx, ProvidesMetrics provider) {
            super(provider);
            this.consumerEx = consumerEx;
        }

        WrappedBiConsumerEx(BiConsumerEx<T, U> consumerEx, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.consumerEx = consumerEx;
        }

        @Override
        public void acceptEx(T t, U u) throws Exception {
            consumerEx.acceptEx(t, u);
        }
    }

    private static class WrappedFunctionEx<T, R> extends AbstractWrapper implements FunctionEx<T, R> {
        private final FunctionEx<T, R> functionEx;

        WrappedFunctionEx(FunctionEx<T, R> functionEx, ProvidesMetrics provider) {
            super(provider);
            this.functionEx = functionEx;
        }

        WrappedFunctionEx(FunctionEx<T, R> functionEx, Collection<ProvidesMetrics> providers) {
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

        WrappedBiFunctionEx(BiFunctionEx<T, U, R> biFunctionEx, ProvidesMetrics provider) {
            super(provider);
            this.biFunctionEx = biFunctionEx;
        }

        @Override
        public R applyEx(T t, U u) throws Exception {
            return biFunctionEx.applyEx(t, u);
        }
    }

    private static class WrappedSupplierEx<P> extends AbstractWrapper implements SupplierEx<P> {
        private final SupplierEx<P> supplierEx;

        WrappedSupplierEx(SupplierEx<P> supplierEx, ProvidesMetrics provider) {
            super(provider);
            this.supplierEx = supplierEx;
        }

        WrappedSupplierEx(SupplierEx<P> supplierEx, Collection<ProvidesMetrics> providers) {
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

        WrappedTriFunction(TriFunction<T0, T1, T2, R> triFunction, ProvidesMetrics provider) {
            super(provider);
            this.triFunction = triFunction;
        }

        @Override
        public R applyEx(T0 t0, T1 t1, T2 t2) throws Exception {
            return triFunction.applyEx(t0, t1, t2);
        }
    }

    private abstract static class AbstractWrapper implements ProvidesMetrics, Serializable {
        private final Collection<ProvidesMetrics> providers;

        AbstractWrapper(ProvidesMetrics provider) {
            this(Collections.singletonList(provider));
        }

        AbstractWrapper(Collection<ProvidesMetrics> providers) {
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
