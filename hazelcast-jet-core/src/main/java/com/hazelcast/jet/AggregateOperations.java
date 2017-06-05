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

package com.hazelcast.jet;

import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.AggregateOperationImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Utility class with factory methods for several useful windowing
 * operations.
 */
public final class AggregateOperations {

    private AggregateOperations() {
    }

    /**
     * Returns an operation that tracks the count of items in the window.
     */
    @Nonnull
    public static AggregateOperation<Object, LongAccumulator, Long> counting() {
        return AggregateOperation.of(
                LongAccumulator::new,
                (a, item) -> a.addExact(1),
                LongAccumulator::addExact,
                LongAccumulator::subtract,
                LongAccumulator::get
        );
    }

    /**
     * Returns an operation that tracks the sum of the quantity returned by
     * {@code mapToLongF} applied to each item in the window.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, LongAccumulator, Long> summingToLong(
            @Nonnull DistributedToLongFunction<T> mapToLongF
    ) {
        return AggregateOperation.of(
                LongAccumulator::new,
                (a, item) -> a.addExact(mapToLongF.applyAsLong(item)),
                LongAccumulator::addExact,
                LongAccumulator::subtractExact,
                LongAccumulator::get
        );
    }

    /**
     * Returns an operation that tracks the sum of the quantity returned by
     * {@code mapToDoubleF} applied to each item in the window.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, DoubleAccumulator, Double> summingToDouble(
            @Nonnull DistributedToDoubleFunction<T> mapToDoubleF
    ) {
        return AggregateOperation.of(
                DoubleAccumulator::new,
                (a, item) -> a.add(mapToDoubleF.applyAsDouble(item)),
                DoubleAccumulator::add,
                DoubleAccumulator::subtract,
                DoubleAccumulator::get
        );
    }

    /**
     * Returns an operation that returns the minimum item, according the given
     * {@code comparator}.
     * <p>
     * The implementation doesn't have the <i>deduction function </i>. {@link
     * AggregateOperation#deductAccumulatorF() See note here}.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, MutableReference<T>, T> minBy(
            @Nonnull DistributedComparator<? super T> comparator
    ) {
        return maxBy(comparator.reversed());
    }

    /**
     * Returns an operation that returns the maximum item, according the given
     * {@code comparator}.
     * <p>
     * The implementation doesn't have the <i>deduction function </i>. {@link
     * AggregateOperation#deductAccumulatorF() See note here}.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, MutableReference<T>, T> maxBy(
            @Nonnull DistributedComparator<? super T> comparator
    ) {
        return AggregateOperation.of(
                MutableReference::new,
                (a, i) -> {
                    if (a.get() == null || comparator.compare(i, a.get()) > 0) {
                        a.set(i);
                    }
                },
                (a1, a2) -> {
                    if (a1.get() == null || comparator.compare(a1.get(), a2.get()) < 0) {
                        a1.set(a2.get());
                    }
                },
                null,
                MutableReference::get
        );
    }

    /**
     * Returns an operation that calculates the arithmetic mean of {@code long}
     * values returned by the {@code mapToLongF} function.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, LongLongAccumulator, Double> averagingLong(
            @Nonnull DistributedToLongFunction<T> mapToLongF
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation.of(
                LongLongAccumulator::new,
                (a, i) -> {
                    if (a.getValue1() == Long.MAX_VALUE) {
                        // this is a bit faster overflow check when we know that we are adding 1
                        throw new ArithmeticException("long overflow");
                    }
                    a.setValue1(a.getValue1() + 1);
                    a.setValue2(Math.addExact(a.getValue2(), mapToLongF.applyAsLong(i)));
                },
                (a1, a2) -> {
                    a1.setValue1(Math.addExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(Math.addExact(a1.getValue2(), a2.getValue2()));
                },
                (a1, a2) -> {
                    a1.setValue1(Math.subtractExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(Math.subtractExact(a1.getValue2(), a2.getValue2()));
                },
                a -> (double) a.getValue2() / a.getValue1()
        );
    }

    /**
     * Returns an operation that calculates the arithmetic mean of {@code double}
     * values returned by the {@code mapToDoubleF} function.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, LongDoubleAccumulator, Double> averagingDouble(
            @Nonnull DistributedToDoubleFunction<T> mapToDoubleF
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation.of(
                LongDoubleAccumulator::new,
                (a, i) -> {
                    if (a.getValue1() == Long.MAX_VALUE) {
                        // this is a bit faster overflow check when we know that we are adding 1
                        throw new ArithmeticException("long overflow");
                    }
                    a.setValue1(a.getValue1() + 1);
                    a.setValue2(a.getValue2() + mapToDoubleF.applyAsDouble(i));
                },
                (a1, a2) -> {
                    a1.setValue1(Math.addExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(a1.getValue2() + a2.getValue2());
                },
                (a1, a2) -> {
                    a1.setValue1(Math.subtractExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(a1.getValue2() - a2.getValue2());
                },
                a -> a.getValue2() / a.getValue1()
        );
    }

    /**
     * Returns an operation that computes a linear trend on the items in the
     * window. The operation will produce a {@code double}-valued coefficient
     * that approximates the rate of change of {@code y} as a function of
     * {@code x}, where {@code x} and {@code y} are {@code long} quantities
     * extracted from each item by the two provided functions.
     */
    @Nonnull
    public static <T> AggregateOperation<T, LinTrendAccumulator, Double> linearTrend(
            @Nonnull DistributedToLongFunction<T> getX,
            @Nonnull DistributedToLongFunction<T> getY
    ) {
        return AggregateOperation.of(
                LinTrendAccumulator::new,
                (a, item) -> a.accumulate(getX.applyAsLong(item), getY.applyAsLong(item)),
                LinTrendAccumulator::combine,
                LinTrendAccumulator::deduct,
                LinTrendAccumulator::finish
        );
    }

    /**
     * Returns an operation, that calculates multiple aggregations and returns their value in
     * {@code List<Object>}.
     * <p>
     * Useful, if you want to calculate multiple values for the same window.
     *
     * @param operations Operations to calculate.
     */
    @SafeVarargs @Nonnull
    public static <T> AggregateOperation<T, List<Object>, List<Object>> allOf(
            @Nonnull AggregateOperation<? super T, ?, ?>... operations
    ) {
        AggregateOperation[] untypedOps = operations;

        return AggregateOperation.of(
                () -> {
                    Object[] res = new Object[untypedOps.length];
                    for (int i = 0; i < untypedOps.length; i++) {
                        res[i] = untypedOps[i].createAccumulatorF().get();
                    }
                    // wrap to List to have equals() implemented
                    return toArrayList(res);
                },
                (accs, item) -> {
                    for (int i = 0; i < untypedOps.length; i++) {
                        untypedOps[i].accumulateItemF().accept(accs.get(i), item);
                    }
                },
                (accs1, accs2) -> {
                    for (int i = 0; i < untypedOps.length; i++) {
                        untypedOps[i].combineAccumulatorsF().accept(accs1.get(i), accs2.get(i));
                    }
                },
                // we can support deduct only if all operations do
                Stream.of(untypedOps).allMatch(o -> o.deductAccumulatorF() != null)
                        ? (accs1, accs2) -> {
                            for (int i = 0; i < untypedOps.length; i++) {
                                untypedOps[i].deductAccumulatorF().accept(accs1.get(i), accs2.get(i));
                            }
                        }
                        : null,
                accs -> {
                    Object[] res = new Object[untypedOps.length];
                    for (int i = 0; i < untypedOps.length; i++) {
                        res[i] = untypedOps[i].finishAccumulationF().apply(accs.get(i));
                    }
                    return toArrayList(res);
                }
        );
    }

    private static <T> ArrayList<T> toArrayList(T[] array) {
        ArrayList<T> res = new ArrayList<>(array.length);
        for (T t : array) {
            res.add(t);
        }
        return res;
    }

    /**
     * A reducing operation maintains an accumulated value that starts out as
     * {@code emptyAccValue} and is being iteratively transformed by applying
     * the {@code combine} primitive to it and each stream item's accumulated
     * value, as returned from {@code toAccValueF}. The {@code combine} must
     * be <em>associative</em> because it will also be used to combine partial
     * results, and <em>commutative</em> because the encounter order of items
     * is unspecified.
     * <p>
     * The optional {@code deduct} primitive allows Jet to compute the sliding
     * window in O(1) time. It must undo the effects of a previous
     * {@code combine}:
     * <pre>
     *     A accVal;  (has some pre-existing value)
     *     A itemAccVal = toAccValueF.apply(item);
     *     A combined = combineAccValuesF.apply(accVal, itemAccVal);
     *     A deducted = deductAccValueF.apply(combined, itemAccVal);
     *     assert deducted.equals(accVal);
     * </pre>
     *
     * @param emptyAccValue the reducing operation's emptyAccValue element
     * @param toAccValueF transforms the stream item into its accumulated value
     * @param combineAccValuesF combines two accumulated values into one
     * @param deductAccValueF deducts the right-hand accumulated value from the left-hand one
     *                        (optional)
     * @param <T> type of the stream item
     * @param <A> type of the accumulated value
     */
    @Nonnull
    public static <T, A> AggregateOperation<T, MutableReference<A>, A> reducing(
            @Nonnull A emptyAccValue,
            @Nonnull DistributedFunction<? super T, ? extends A> toAccValueF,
            @Nonnull DistributedBinaryOperator<A> combineAccValuesF,
            @Nullable DistributedBinaryOperator<A> deductAccValueF
    ) {
        return new AggregateOperationImpl<>(
                () -> new MutableReference<>(emptyAccValue),
                (a, t) -> a.set(combineAccValuesF.apply(a.get(), toAccValueF.apply(t))),
                (a, b) -> a.set(combineAccValuesF.apply(a.get(), b.get())),
                deductAccValueF != null ? (a, b) -> a.set(deductAccValueF.apply(a.get(), b.get())) : null,
                MutableReference::get);
    }
}
