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
import com.hazelcast.jet.core.metrics.ProvidesMetrics;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public final class UserMetricsUtil {

    private static final ProvidesMetrics NOOP_METRICS_PROVIDER = (ProvidesMetrics & Serializable) context -> { };

    private UserMetricsUtil() {
    }

    public static ProvidesMetrics cast(Object candidate) {
        if (candidate instanceof ProvidesMetrics) {
            return (ProvidesMetrics) candidate;
        } else {
            return NOOP_METRICS_PROVIDER;
        }
    }

    public static <T, U> BiPredicateEx<T, U> wrapPredicate(BiPredicateEx<T, U> biPredicateEx, Object candidate) {
        if (notAlreadyWrapped(biPredicateEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedBiPredicateEx<>(biPredicateEx, (ProvidesMetrics) candidate);
        }
        return biPredicateEx;
    }

    public static <T, U> BiConsumerEx<T, U> wrapConsumer(BiConsumerEx<T, U> biConsumerEx, Object candidate) {
        if (notAlreadyWrapped(biConsumerEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedBiConsumerEx<>(biConsumerEx, (ProvidesMetrics) candidate);
        }
        return biConsumerEx;
    }

    public static <T, U> BiConsumerEx<T, U> wrapAll(BiConsumerEx<T, U> biConsumerEx,
                                                    List<?> metricsProviderCandidates) {
        return wrapAll(biConsumerEx, getProviders(metricsProviderCandidates.stream()));
    }

    public static <T, U> BiConsumerEx<T, U> wrapAll(BiConsumerEx<T, U> biConsumerEx, Object[] candidates) {
        return wrapAll(biConsumerEx, getProviders(Arrays.stream(candidates)));
    }

    private static <T, U> BiConsumerEx<T, U> wrapAll(BiConsumerEx<T, U> biConsumerEx, Set<ProvidesMetrics> providers) {
        if (notAlreadyWrapped(biConsumerEx) && !providers.isEmpty()) {
            return new WrappedBiConsumerEx<>(biConsumerEx, providers);
        }
        return biConsumerEx;
    }

    public static <T, R> FunctionEx<T, R> wrap(FunctionEx<T, R> functionEx, Object candidate) {
        if (notAlreadyWrapped(functionEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedFunctionEx<>(functionEx, (ProvidesMetrics) candidate);
        }
        return functionEx;
    }

    public static <T> PredicateEx<T> wrapPredicate(PredicateEx<T> predicateEx, Object candidate) {
        if (notAlreadyWrapped(predicateEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedPredicateEx<>(predicateEx, (ProvidesMetrics) candidate);
        }
        return predicateEx;
    }

    public static <T, R> FunctionEx<T, R> wrapAll(FunctionEx<T, R> functionEx, List<?> candidates) {
        return wrapAll(functionEx, getProviders(candidates.stream()));
    }

    public static <T, R> FunctionEx<T, R> wrapAll(FunctionEx<T, R> functionEx, Object[] candidates) {
        return wrapAll(functionEx, getProviders(Arrays.stream(candidates)));
    }

    private static <T, R> FunctionEx<T, R> wrapAll(FunctionEx<T, R> functionEx, Set<ProvidesMetrics> providers) {
        if (notAlreadyWrapped(functionEx) && !providers.isEmpty()) {
            return new WrappedFunctionEx<>(functionEx, providers);
        }
        return functionEx;
    }

    public static <T, U, R> BiFunctionEx<T, U, R> wrap(BiFunctionEx<T, U, R> biFunctionEx, Object candidate) {
        if (notAlreadyWrapped(biFunctionEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedBiFunctionEx<>(biFunctionEx, (ProvidesMetrics) candidate);
        }
        return biFunctionEx;
    }

    public static <T, U, R> BiFunctionEx<T, U, R> wrapAll(BiFunctionEx<T, U, R> biFunctionEx, List<?> candidates) {
        return wrapAll(biFunctionEx, getProviders(candidates.stream()));
    }

    private static <T, U, R> BiFunctionEx<T, U, R> wrapAll(BiFunctionEx<T, U, R> biFunctionEx,
                                                           Set<ProvidesMetrics> providers) {
        if (notAlreadyWrapped(biFunctionEx) && !providers.isEmpty()) {
            return new WrappedBiFunctionEx<>(biFunctionEx, providers);
        }
        return biFunctionEx;
    }

    public static <T0, T1, T2, R> TriFunction<T0, T1, T2, R> wrap(TriFunction<T0, T1, T2, R> triFunction,
                                                                  Object candidate) {
        if (notAlreadyWrapped(triFunction) && candidate instanceof ProvidesMetrics) {
            return new WrappedTriFunction<>(triFunction, (ProvidesMetrics) candidate);
        }
        return triFunction;
    }

    public static <T0, T1, T2, R> TriFunction<T0, T1, T2, R> wrapAll(
            TriFunction<T0, T1, T2, R> triFunctionEx, List<?> candidates) {
        return wrapAll(triFunctionEx, getProviders(candidates.stream()));
    }

    private static <T0, T1, T2, R> TriFunction<T0, T1, T2, R> wrapAll(TriFunction<T0, T1, T2, R> triFunctionEx,
                                                           Set<ProvidesMetrics> providers) {
        if (notAlreadyWrapped(triFunctionEx) && !providers.isEmpty()) {
            return new WrappedTriFunction<>(triFunctionEx, providers);
        }
        return triFunctionEx;
    }

    public static <P> SupplierEx<P> wrap(SupplierEx<P> supplierEx, Object candidate) {
        if (notAlreadyWrapped(supplierEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedSupplierEx<>(supplierEx, (ProvidesMetrics) candidate);
        }
        return supplierEx;
    }

    public static <P> SupplierEx<P> wrapAll(SupplierEx<P> supplierEx, List<?> candidates) {
        Set<ProvidesMetrics> providers = getProviders(candidates.stream());
        return wrapAll(supplierEx, providers);
    }

    public static <P> SupplierEx<P> wrapAll(SupplierEx<P> supplierEx, Object[] candidates) {
        Set<ProvidesMetrics> providers = getProviders(Arrays.stream(candidates));
        return wrapAll(supplierEx, providers);
    }

    private static <P> SupplierEx<P> wrapAll(SupplierEx<P> supplierEx, Set<ProvidesMetrics> providers) {
        if (notAlreadyWrapped(supplierEx) && !providers.isEmpty()) {
            return new WrappedSupplierEx<>(supplierEx, providers);
        }
        return supplierEx;
    }

    private static Set<ProvidesMetrics> getProviders(Stream<?> candidates) {
        return candidates
                .filter(ProvidesMetrics.class::isInstance)
                .map(ProvidesMetrics.class::cast)
                .collect(toSet());
    }

    private static boolean notAlreadyWrapped(Object obj) {
        return !(obj instanceof AbstractWrapper);
    }

    public static class WrappedPredicateEx<T> extends AbstractWrapper implements PredicateEx<T> {
        private final PredicateEx<T> predicateEx;

        public WrappedPredicateEx(PredicateEx<T> predicateEx, ProvidesMetrics provider) {
            super(provider);
            this.predicateEx = predicateEx;
        }

        @Override
        public boolean testEx(T t) throws Exception {
            return predicateEx.testEx(t);
        }
    }

    public static class WrappedBiPredicateEx<T, U> extends AbstractWrapper implements BiPredicateEx<T, U> {
        private final BiPredicateEx<T, U> predicateEx;

        public WrappedBiPredicateEx(BiPredicateEx<T, U> predicateEx, ProvidesMetrics provider) {
            super(provider);
            this.predicateEx = predicateEx;
        }

        @Override
        public boolean testEx(T t, U u) throws Exception {
            return predicateEx.testEx(t, u);
        }
    }

    public static class WrappedBiConsumerEx<T, U> extends AbstractWrapper implements BiConsumerEx<T, U> {
        private final BiConsumerEx<T, U> consumerEx;

        public WrappedBiConsumerEx(BiConsumerEx<T, U> consumerEx, ProvidesMetrics provider) {
            super(provider);
            this.consumerEx = consumerEx;
        }

        public WrappedBiConsumerEx(BiConsumerEx<T, U> consumerEx, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.consumerEx = consumerEx;
        }

        @Override
        public void acceptEx(T t, U u) throws Exception {
            consumerEx.acceptEx(t, u);
        }
    }

    public static class WrappedFunctionEx<T, R> extends AbstractWrapper implements FunctionEx<T, R> {
        private final FunctionEx<T, R> functionEx;

        public WrappedFunctionEx(FunctionEx<T, R> functionEx, ProvidesMetrics provider) {
            super(provider);
            this.functionEx = functionEx;
        }

        public WrappedFunctionEx(FunctionEx<T, R> functionEx, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.functionEx = functionEx;
        }

        @Override
        public R applyEx(T t) throws Exception {
            return functionEx.applyEx(t);
        }
    }

    public static class WrappedBiFunctionEx<T, U, R> extends AbstractWrapper implements BiFunctionEx<T, U, R> {
        private final BiFunctionEx<T, U, R> biFunctionEx;

        public WrappedBiFunctionEx(BiFunctionEx<T, U, R> biFunctionEx, ProvidesMetrics provider) {
            super(provider);
            this.biFunctionEx = biFunctionEx;
        }

        public WrappedBiFunctionEx(BiFunctionEx<T, U, R> biFunctionEx, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.biFunctionEx = biFunctionEx;
        }

        @Override
        public R applyEx(T t, U u) throws Exception {
            return biFunctionEx.applyEx(t, u);
        }
    }

    public static class WrappedSupplierEx<P> extends AbstractWrapper implements SupplierEx<P> {
        private final SupplierEx<P> supplierEx;

        public WrappedSupplierEx(SupplierEx<P> supplierEx, ProvidesMetrics provider) {
            super(provider);
            this.supplierEx = supplierEx;
        }

        public WrappedSupplierEx(SupplierEx<P> supplierEx, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.supplierEx = supplierEx;
        }

        @Override
        public P getEx() throws Exception {
            return supplierEx.getEx();
        }
    }

    public static class WrappedTriFunction<T0, T1, T2, R> extends AbstractWrapper implements TriFunction<T0, T1, T2, R> {
        private final TriFunction<T0, T1, T2, R> triFunction;

        public WrappedTriFunction(TriFunction<T0, T1, T2, R> triFunction, ProvidesMetrics provider) {
            super(provider);
            this.triFunction = triFunction;
        }

        public WrappedTriFunction(TriFunction<T0, T1, T2, R> triFunction, Collection<ProvidesMetrics> providers) {
            super(providers);
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
