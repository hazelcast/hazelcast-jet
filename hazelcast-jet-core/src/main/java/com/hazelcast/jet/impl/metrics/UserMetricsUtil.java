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
import com.hazelcast.jet.core.metrics.MetricsProvider;
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
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public final class UserMetricsUtil {

    private static final MetricsProvider NOOP_METRICS_PROVIDER = (MetricsProvider & Serializable) context -> { };

    private UserMetricsUtil() {
    }

    public static MetricsProvider cast(Object candidate) {
        if (candidate instanceof MetricsProvider) {
            return (MetricsProvider) candidate;
        } else {
            return NOOP_METRICS_PROVIDER;
        }
    }

    public static <P> SupplierEx<P> wrapSupplier(SupplierEx<P> supplierEx, Object... candidates) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(supplierEx) || providers.isEmpty()) {
            return supplierEx;
        }
        return new WrappedSupplierEx<>(supplierEx, providers);
    }

    public static <T> PredicateEx<T> wrapPredicate(PredicateEx<T> predicateEx, Object... candidates) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(predicateEx) || providers.isEmpty()) {
            return predicateEx;
        }
        return new WrappedPredicateEx<>(predicateEx, providers);
    }

    public static <T, U> BiPredicateEx<T, U> wrapBiPredicate(BiPredicateEx<T, U> biPredicateEx, Object... candidates) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(biPredicateEx) || providers.isEmpty()) {
            return biPredicateEx;
        }
        return new WrappedBiPredicateEx<>(biPredicateEx, providers);
    }

    public static <T, U> BiConsumerEx<T, U> wrapBiConsumer(BiConsumerEx<T, U> biConsumerEx, Object... candidates) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(biConsumerEx) || providers.isEmpty()) {
            return biConsumerEx;
        }
        return new WrappedBiConsumerEx<>(biConsumerEx, providers);
    }

    public static <T, R> FunctionEx<T, R> wrapFunction(FunctionEx<T, R> functionEx, Object... candidates) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(functionEx) || providers.isEmpty()) {
            return functionEx;
        }
        return new WrappedFunctionEx<>(functionEx, providers);
    }

    public static <T, U, R> BiFunctionEx<T, U, R> wrapBiFunction(BiFunctionEx<T, U, R> biFunction, Object... candidates) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(biFunction) || providers.isEmpty()) {
            return biFunction;
        }
        return new WrappedBiFunctionEx<>(biFunction, providers);
    }

    public static <T0, T1, T2, R> TriFunction<T0, T1, T2, R> wrapTriFunction(
            TriFunction<T0, T1, T2, R> triFunctionEx, Object... candidates
    ) {
        Set<MetricsProvider> providers = getProviders(Arrays.stream(candidates));
        if (alreadyWrapped(triFunctionEx) || providers.isEmpty()) {
            return triFunctionEx;
        }
        return new WrappedTriFunction<>(triFunctionEx, providers);
    }

    private static Set<MetricsProvider> getProviders(Stream<?> candidates) {
        return candidates
                .filter(MetricsProvider.class::isInstance)
                .map(MetricsProvider.class::cast)
                .collect(toSet());
    }

    private static boolean alreadyWrapped(Object obj) {
        return obj instanceof AbstractWrapper;
    }

    private static class WrappedPredicateEx<T> extends AbstractWrapper implements PredicateEx<T> {
        private final PredicateEx<T> predicateEx;

        WrappedPredicateEx(PredicateEx<T> predicateEx, Collection<MetricsProvider> provider) {
            super(provider);
            this.predicateEx = predicateEx;
        }

        @Override
        public boolean testEx(T t) throws Exception {
            return predicateEx.testEx(t);
        }
    }

    private static class WrappedBiPredicateEx<T, U> extends AbstractWrapper implements BiPredicateEx<T, U> {
        private final BiPredicateEx<T, U> predicateEx;

        WrappedBiPredicateEx(BiPredicateEx<T, U> predicateEx, Collection<MetricsProvider> provider) {
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

        WrappedBiConsumerEx(BiConsumerEx<T, U> consumerEx, Collection<MetricsProvider> providers) {
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

        WrappedFunctionEx(FunctionEx<T, R> functionEx, Collection<MetricsProvider> providers) {
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

        WrappedBiFunctionEx(BiFunctionEx<T, U, R> biFunctionEx, Collection<MetricsProvider> providers) {
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

        WrappedSupplierEx(SupplierEx<P> supplierEx, Collection<MetricsProvider> providers) {
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

        WrappedTriFunction(TriFunction<T0, T1, T2, R> triFunction, Collection<MetricsProvider> providers) {
            super(providers);
            this.triFunction = triFunction;
        }

        @Override
        public R applyEx(T0 t0, T1 t1, T2 t2) throws Exception {
            return triFunction.applyEx(t0, t1, t2);
        }
    }

    private abstract static class AbstractWrapper implements MetricsProvider, Serializable {
        private final Collection<MetricsProvider> providers;

        AbstractWrapper(Collection<MetricsProvider> providers) {
            this.providers = providers;
        }

        @Override
        public void registerMetrics(MetricsContext context) {
            for (MetricsProvider provider : providers) {
                provider.registerMetrics(context);
            }
        }
    }
}
