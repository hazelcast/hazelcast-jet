/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation2Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation3Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;
import com.hazelcast.jet.pipeline.datamodel.Tag;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * A builder object that can be used to construct the definition of an
 * aggregate operation in a step-by-step manner. Please refer to
 * {@link AggregateOperation#withCreate(DistributedSupplier)
 * AggregateOperation.withCreate()} for more details.
 *
 * @param <A> the type of the accumulator
 */
public final class AggregateOperationBuilder<A> {

    @Nonnull
    private final DistributedSupplier<A> createFn;

    AggregateOperationBuilder(@Nonnull DistributedSupplier<A> createFn) {
        this.createFn = createFn;
    }

    /**
     * Registers the supplied {@code accumulate} primitive. Also selects the
     * fixed-arity variant of the aggregate operation.
     * <p>
     * This method is synonymous with {@link #andAccumulate0(
     * DistributedBiConsumer)}, but makes more sense when defining a
     * simple, arity-1 aggregate operation.
     *
     * @param accumulateFn the {@code accumulate} primitive
     * @param <T> the expected type of input item
     * @return a new builder object that captures the {@code T0} type parameter
     */
    @Nonnull
    public <T> Arity1<T, A> andAccumulate(@Nonnull DistributedBiConsumer<? super A, T> accumulateFn) {
        return new Arity1<>(createFn, accumulateFn);
    }

    /**
     * Registers the supplied {@code accumulate} primitive for stream-0.
     * Also selects the fixed-arity variant of the aggregate operation.
     *
     * @param accumulateFn0 the {@code accumulate} primitive for stream-0
     * @param <T0> the expected type of item in stream-0
     * @return a new builder object that captures the {@code T0} type parameter
     */
    @Nonnull
    public <T0> Arity1<T0, A> andAccumulate0(@Nonnull DistributedBiConsumer<? super A, T0> accumulateFn0) {
        return new Arity1<>(createFn, accumulateFn0);
    }

    /**
     * Registers the supplied {@code accumulate} primitive for the stream tagged
     * with the supplied tag. Also selects the variable-arity variant of the
     * aggregate operation.
     *
     * @param tag the tag of the associated input stream
     * @param accumulateFn the {@code accumulate} primitive
     * @param <T> the expected type of input item
     * @return a new builder object for variable-arity aggregate operations
     */
    @Nonnull
    public <T> VarArity<A> andAccumulate(
            @Nonnull Tag<T> tag, @Nonnull DistributedBiConsumer<? super A, T> accumulateFn
    ) {
        return new VarArity<>(createFn, tag, accumulateFn);
    }

    /**
     * The arity-1 variant of the aggregate operation builder. Can be
     * raised to arity-2 by calling {@link #andAccumulate1(
     * DistributedBiConsumer) andAccumulate1()}.
     *
     * @param <T0> the type of item in stream-0
     * @param <A> the type of the accumulator
     */
    public static class Arity1<T0, A> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        @Nonnull
        private final DistributedBiConsumer<? super A, T0> accumulateFn0;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        Arity1(@Nonnull DistributedSupplier<A> createFn, @Nonnull DistributedBiConsumer<? super A, T0> accumulateFn0) {
            this.createFn = createFn;
            this.accumulateFn0 = accumulateFn0;
        }

        /**
         * Registers the supplied {@code accumulate} primitive for stream-1.
         * Also selects the fixed-arity variant of the aggregate operation.
         *
         * @param accumulateFn1 the {@code accumulate} primitive for stream-1
         * @param <T1> the expected type of item in stream-1
         * @return a new builder object that captures the {@code T1} type parameter
         */
        @Nonnull
        public <T1> Arity2<T0, T1, A> andAccumulate1(@Nonnull DistributedBiConsumer<? super A, T1> accumulateFn1) {
            return new Arity2<>(this, accumulateFn1);
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity1<T0, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity1<T0, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Constructs and returns an {@link AggregateOperation1} from the
         * current state of the builder and the supplied {@code finish} primitive.
         */
        @Nonnull
        public <R> AggregateOperation1<T0, A, R> andFinish(@Nonnull DistributedFunction<? super A, R> finishFn) {
            return new AggregateOperation1Impl<>(createFn, accumulateFn0, combineFn, deductFn, finishFn);
        }

        /**
         * Constructs and returns an {@link AggregateOperation1} from the current
         * state of the builder, with the identity function as the {@code finish}
         * primitive.
         */
        @Nonnull
        public AggregateOperation1<T0, A, A> andIdentityFinish() {
            return new AggregateOperation1Impl<>(createFn, accumulateFn0, combineFn, deductFn,
                    DistributedFunction.identity());
        }
    }

    /**
     * The arity-2 variant of the aggregate operation builder. Can be
     * raised to arity-3 by calling {@link #andAccumulate2(
     * DistributedBiConsumer) andAccumulate2()}.
     *
     * @param <T0> the type of item in stream-0
     * @param <T1> the type of item in stream-1
     * @param <A> the type of the accumulator
     */
    public static class Arity2<T0, T1, A> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        @Nonnull
        private final DistributedBiConsumer<? super A, T0> accumulateFn0;
        @Nonnull
        private final DistributedBiConsumer<? super A, T1> accumulateFn1;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        Arity2(@Nonnull Arity1<T0, A> step1, @Nonnull DistributedBiConsumer<? super A, T1> accumulateFn1) {
            this.createFn = step1.createFn;
            this.accumulateFn0 = step1.accumulateFn0;
            this.accumulateFn1 = accumulateFn1;
        }

        /**
         * Registers the supplied {@code accumulate} primitive for stream-2.
         * Also selects the fixed-arity variant of the aggregate operation.
         *
         * @param accumulateFn2 the {@code accumulate} primitive for stream-2
         * @param <T2> the expected type of item in stream-2
         * @return a new builder object that captures the {@code T2} type parameter
         */
        @Nonnull
        public <T2> Arity3<T0, T1, T2, A> andAccumulate2(@Nonnull DistributedBiConsumer<? super A, T2> accumulateFn2) {
            return new Arity3<>(this, accumulateFn2);
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity2<T0, T1, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public Arity2<T0, T1, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Constructs and returns an {@link AggregateOperation2} from the
         * current state of the builder and the supplied {@code finish} primitive.
         */
        @Nonnull
        public <R> AggregateOperation2<T0, T1, A, R> andFinish(@Nonnull DistributedFunction<? super A, R> finishFn) {
            return new AggregateOperation2Impl<>(createFn,
                    accumulateFn0, accumulateFn1,
                    combineFn, deductFn, finishFn);
        }

        /**
         * Constructs and returns an {@link AggregateOperation2} from the current
         * state of the builder, with the identity function as the {@code
         * finish} primitive.
         */
        @Nonnull
        public AggregateOperation2<T0, T1, A, A> andIdentityFinish() {
            return new AggregateOperation2Impl<>(createFn,
                    accumulateFn0, accumulateFn1,
                    combineFn, deductFn, DistributedFunction.identity());
        }
    }

    /**
     * The arity-3 variant of the aggregate operation builder.
     *
     * @param <T0> the type of item in stream-0
     * @param <T1> the type of item in stream-1
     * @param <T2> the type of item in stream-2
     * @param <A> the type of the accumulator
     */
    public static class Arity3<T0, T1, T2, A> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        @Nonnull
        private final DistributedBiConsumer<? super A, T0> accumulateFn0;
        @Nonnull
        private final DistributedBiConsumer<? super A, T1> accumulateFn1;
        @Nonnull
        private final DistributedBiConsumer<? super A, T2> accumulateFn2;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        Arity3(Arity2<T0, T1, A> step2,
               DistributedBiConsumer<? super A, T2> accumulateFn2
        ) {
            this.createFn = step2.createFn;
            this.accumulateFn0 = step2.accumulateFn0;
            this.accumulateFn1 = step2.accumulateFn1;
            this.accumulateFn2 = accumulateFn2;
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity3<T0, T1, T2, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public Arity3<T0, T1, T2, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Constructs and returns an {@link AggregateOperation3} from the
         * current state of the builder and the supplied {@code finish} primitive.
         */
        @Nonnull
        public <R> AggregateOperation3<T0, T1, T2, A, R> andFinish(
                @Nonnull DistributedFunction<? super A, R> finishFn
        ) {
            return new AggregateOperation3Impl<>(createFn,
                    accumulateFn0, accumulateFn1, accumulateFn2,
                    combineFn, deductFn, finishFn);
        }

        /**
         * Constructs and returns an {@link AggregateOperation3} from the current
         * state of the builder, with the identity function as the {@code
         * finish} primitive.
         */
        @Nonnull
        public AggregateOperation3<T0, T1, T2, A, A> andIdentityFinish() {
            return new AggregateOperation3Impl<>(createFn,
                    accumulateFn0, accumulateFn1, accumulateFn2,
                    combineFn, deductFn, DistributedFunction.identity());
        }
    }

    /**
     * The variable-arity variant of the aggregate operation builder.
     * Accepts any number of {@code accumulate} primitives and associates
     * them with {@link Tag}s. Does not capture the static type of the
     * stream items.
     *
     * @param <A> the type of the accumulator
     */
    public static class VarArity<A> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        private final Map<Integer, DistributedBiConsumer<? super A, ?>> accumulateFnsByTag = new HashMap<>();
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        <T> VarArity(
                @Nonnull DistributedSupplier<A> createFn,
                @Nonnull Tag<T> tag,
                @Nonnull DistributedBiConsumer<? super A, T> accumulateFn
        ) {
            this.createFn = createFn;
            accumulateFnsByTag.put(tag.index(), accumulateFn);
        }

        /**
         * Registers the supplied {@code accumulate} primitive for the stream tagged
         * with the supplied tag.
         *
         * @param tag the tag of the associated input stream
         * @param accumulateFn the {@code accumulate} primitive
         * @param <T> the expected type of input item
         * @return a new builder object for variable-arity aggregate operations
         */
        @Nonnull
        public <T> VarArity<A> andAccumulate(
                @Nonnull Tag<T> tag, @Nonnull DistributedBiConsumer<? super A, T> accumulateFn
        ) {
            accumulateFnsByTag.merge(tag.index(), accumulateFn, (x, y) -> {
                throw new IllegalArgumentException("Tag with index " + tag.index() + " already registered");
            });
            return this;
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public VarArity<A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public VarArity<A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Constructs and returns an {@link AggregateOperation} from the
         * current state of the builder and the supplied {@code finish} primitive.
         */
        @Nonnull
        public <R> AggregateOperation<A, R> andFinish(@Nonnull DistributedFunction<? super A, R> finishFn) {
            checkNotNull(finishFn, "finishFn");
            return new AggregateOperationImpl<>(createFn, packAccumulateFns(),
                    combineFn, deductFn, finishFn);
        }

        /**
         * Constructs and returns an {@link AggregateOperation} from the current
         * state of the builder, with the identity function as the {@code
         * finish} primitive.
         */
        @Nonnull
        public AggregateOperation<A, A> andIdentityFinish() {
            return new AggregateOperationImpl<>(createFn, packAccumulateFns(),
                    combineFn, deductFn, DistributedFunction.identity());
        }

        private DistributedBiConsumer<? super A, ?>[] packAccumulateFns() {
            int size = accumulateFnsByTag.size();
            @SuppressWarnings("unchecked")
            DistributedBiConsumer<? super A, ?>[] accFs = new DistributedBiConsumer[size];
            for (int i = 0; i < size; i++) {
                accFs[i] = accumulateFnsByTag.get(i);
                if (accFs[i] == null) {
                    throw new IllegalStateException("Registered tags' indices are "
                            + accumulateFnsByTag.keySet().stream().sorted().collect(toList())
                            + " but should be " + range(0, size).boxed().collect(toList()));
                }
            }
            return accFs;
        }
    }
}
