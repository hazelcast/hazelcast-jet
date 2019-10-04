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
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public final class UserMetricsUtil {

    private static final ProvidesMetrics NOOP_METRICS_PROVIDER = context -> { };

    private UserMetricsUtil() {
    }

    public static ProvidesMetrics cast(Object candidate) {
        if (candidate instanceof ProvidesMetrics) {
            return (ProvidesMetrics) candidate;
        } else {
            return NOOP_METRICS_PROVIDER;
        }
    }

    public static <T, U> BiPredicateEx<T, U> wrap(BiPredicateEx<T, U> biPredicateEx, Object candidate) {
        if (notAlreadyWrapped(biPredicateEx) && candidate instanceof ProvidesMetrics) {
            return new WrappedBiPredicateEx<>(biPredicateEx, (ProvidesMetrics) candidate);
        }
        return biPredicateEx;
    }

    public static <T, U> BiConsumerEx<T, U> wrap(BiConsumerEx<T, U> biConsumerEx, Object candidate) {
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

    @SuppressWarnings("unchecked")
    public static <T extends AggregateOperation> T wrap(T aggrOp, Object candidate) {
        if (notAlreadyWrapped(aggrOp) && candidate instanceof ProvidesMetrics) {
            if (aggrOp.arity() == 1) {
                return (T) new WrappedAggregateOperation1((AggregateOperation1) aggrOp,
                        (ProvidesMetrics) candidate);
            } else if (aggrOp.arity() == 2) {
                return (T) new WrappedAggregateOperation2((AggregateOperation2) aggrOp,
                        (ProvidesMetrics) candidate);
            } else if (aggrOp.arity() == 3) {
                return (T) new WrappedAggregateOperation3((AggregateOperation3) aggrOp,
                        (ProvidesMetrics) candidate);
            } else {
                return (T) new WrappedAggregateOperation(aggrOp, (ProvidesMetrics) candidate);
            }
        }
        return aggrOp;
    }

    @SuppressWarnings("unchecked")
    public static <T extends AggregateOperation> T wrapAll(T op) {
        if (notAlreadyWrapped(op)) {
            Set<ProvidesMetrics> providers = getProviders(getCandidates(op).stream());
            if (!providers.isEmpty()) {
                if (op.arity() == 1) {
                    return (T) new WrappedAggregateOperation1((AggregateOperation1) op, providers);
                } else if (op.arity() == 2) {
                    return (T) new WrappedAggregateOperation2((AggregateOperation2) op, providers);
                } else if (op.arity() == 3) {
                    return (T) new WrappedAggregateOperation3((AggregateOperation3) op, providers);
                } else {
                    return (T) new WrappedAggregateOperation(op, providers);
                }
            }
        }
        return op;
    }

    private static List<?> getCandidates(AggregateOperation op) {
        List<Object> candidates = new ArrayList<>();
        for (int i = 0; i < op.arity(); i++) {
            candidates.add(op.accumulateFn(i));
        }
        candidates.add(op.createFn());
        candidates.add(op.combineFn());
        candidates.add(op.deductFn());
        candidates.add(op.exportFn());
        candidates.add(op.finishFn());
        return candidates;
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

    @SuppressWarnings("unchecked")
    private static class WrappedAggregateOperation extends AbstractWrapper implements AggregateOperation {
        private final AggregateOperation aggrOp;

        WrappedAggregateOperation(AggregateOperation aggrOp, ProvidesMetrics provider) {
            super(provider);
            this.aggrOp = aggrOp;
        }

        WrappedAggregateOperation(AggregateOperation aggrOp, Collection<ProvidesMetrics> providers) {
            super(providers);
            this.aggrOp = aggrOp;
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn(@Nonnull Tag tag) {
            return aggrOp.accumulateFn(tag);
        }

        @Nonnull
        @Override
        public AggregateOperation1 withCombiningAccumulateFn(@Nonnull FunctionEx getAccFn) {
            return aggrOp.withCombiningAccumulateFn(getAccFn);
        }

        @Nonnull
        @Override
        public AggregateOperation withIdentityFinish() {
            return aggrOp.withIdentityFinish();
        }

        @Nonnull
        @Override
        public AggregateOperation andThen(FunctionEx thenFn) {
            return aggrOp.andThen(thenFn);
        }

        @Override
        public int arity() {
            return aggrOp.arity();
        }

        @Nonnull
        @Override
        public SupplierEx createFn() {
            return aggrOp.createFn();
        }

        @Nonnull
        @Override
        public BiConsumerEx accumulateFn(int index) {
            return aggrOp.accumulateFn(index);
        }

        @Nullable
        @Override
        public BiConsumerEx combineFn() {
            return aggrOp.combineFn();
        }

        @Nullable
        @Override
        public BiConsumerEx deductFn() {
            return aggrOp.deductFn();
        }

        @Nonnull
        @Override
        public FunctionEx exportFn() {
            return aggrOp.exportFn();
        }

        @Nonnull
        @Override
        public FunctionEx finishFn() {
            return aggrOp.finishFn();
        }

        @Nonnull
        @Override
        public AggregateOperation withAccumulateFns(BiConsumerEx... accumulateFns) {
            return aggrOp.withAccumulateFns(accumulateFns);
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

        WrappedBiFunctionEx(BiFunctionEx<T, U, R> biFunctionEx, Collection<ProvidesMetrics> providers) {
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

        WrappedTriFunction(TriFunction<T0, T1, T2, R> triFunction, Collection<ProvidesMetrics> providers) {
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
