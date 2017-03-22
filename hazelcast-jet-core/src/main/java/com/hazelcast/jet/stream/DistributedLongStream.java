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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.config.JobConfig;

import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * An extension of {@link java.util.stream.LongStream} to support distributed stream operations by replacing
 * functional interfaces with their serializable equivalents.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedLongStream extends LongStream {

    /**
     * Returns a stream consisting of the elements of this stream that match
     * the given predicate.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to each element to determine if it
     *                  should be included
     * @return the new stream
     */
    default DistributedLongStream filter(Distributed.LongPredicate predicate) {
        return filter((LongPredicate) predicate);
    }

    /**
     * Returns a stream consisting of the results of applying the given
     * function to the elements of this stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    default DistributedLongStream map(Distributed.LongUnaryOperator mapper) {
        return map((LongUnaryOperator) mapper);
    }

    /**
     * Returns an object-valued {@code Stream} consisting of the results of
     * applying the given function to the elements of this stream.
     *
     * <p>This is an
     *     intermediate operation.
     *
     * @param <U> the element type of the new stream
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    default <U> DistributedStream<U> mapToObj(Distributed.LongFunction<? extends U> mapper) {
        return mapToObj((LongFunction<? extends U>) mapper);
    }

    /**
     * Returns an {@code IntStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    default DistributedIntStream mapToInt(Distributed.LongToIntFunction mapper) {
        return mapToInt((LongToIntFunction) mapper);
    }

    /**
     * Returns a {@code DoubleStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    default DistributedDoubleStream mapToDouble(Distributed.LongToDoubleFunction mapper) {
        return mapToDouble((LongToDoubleFunction) mapper);
    }

    /**
     * Returns a stream consisting of the results of replacing each element of
     * this stream with the contents of a mapped stream produced by applying
     * the provided mapping function to each element.  Each mapped stream is
     * {@link java.util.stream.BaseStream#close() closed} after its contents
     * have been placed into this stream.  (If a mapped stream is {@code null}
     * an empty stream is used, instead.)
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element which produces a
     *               {@code LongStream} of new values
     * @return the new stream
     * @see DistributedStream#flatMap(Distributed.Function)
     */
    default DistributedLongStream flatMap(Distributed.LongFunction<? extends LongStream> mapper) {
        return flatMap((LongFunction<? extends LongStream>) mapper);
    }

    /**
     * Returns a stream consisting of the distinct elements of this stream.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @return the new stream
     */
    @Override
    DistributedLongStream distinct();

    /**
     * Returns a stream consisting of the elements of this stream in sorted
     * order.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @return the new stream
     */
    @Override
    DistributedLongStream sorted();

    /**
     * Returns a stream consisting of the elements of this stream, additionally
     * performing the provided action on each element as elements are consumed
     * from the resulting stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * <p>For parallel stream pipelines, the action may be called at
     * whatever time and in whatever thread the element is made available by the
     * upstream operation.  If the action modifies shared state,
     * it is responsible for providing the required synchronization.
     *
     * @param action a
     *               non-interfering action to perform on the elements as
     *               they are consumed from the stream
     * @return the new stream
     */
    default DistributedLongStream peek(Distributed.LongConsumer action) {
        return peek((LongConsumer) action);
    }

    /**
     * Returns a stream consisting of the elements of this stream, truncated
     * to be no longer than {@code maxSize} in length.
     *
     * <p>This is a short-circuiting
     * stateful intermediate operation.
     *
     * @param maxSize the number of elements the stream should be limited to
     * @return the new stream
     * @throws IllegalArgumentException if {@code maxSize} is negative
     */
    DistributedLongStream limit(long maxSize);

    /**
     * Returns a stream consisting of the remaining elements of this stream
     * after discarding the first {@code n} elements of the stream.
     * If this stream contains fewer than {@code n} elements then an
     * empty stream will be returned.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @param n the number of leading elements to skip
     * @return the new stream
     * @throws IllegalArgumentException if {@code n} is negative
     */
    DistributedLongStream skip(long n);

    /**
     * Performs a reduction on the
     * elements of this stream, using the provided identity value and an
     * associative
     * accumulation function, and returns the reduced value.  This is equivalent
     * to:
     * <pre>{@code
     *     long result = identity;
     *     for (long element : this stream)
     *         result = accumulator.applyAsLong(result, element)
     *     return result;
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code identity} value must be an identity for the accumulator
     * function. This means that for all {@code x},
     * {@code accumulator.apply(identity, x)} is equal to {@code x}.
     * The {@code accumulator} function must be an
     * associative function.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param identity the identity value for the accumulating function
     * @param op an associative,
     *           non-interfering,
     *           stateless
     *           function for combining two values
     * @return the result of the reduction
     * @see #sum()
     * @see #min()
     * @see #max()
     * @see #average()
     */
    default long reduce(long identity, Distributed.LongBinaryOperator op) {
        return reduce(identity, (LongBinaryOperator) op);
    }

    /**
     * Performs a reduction on the
     * elements of this stream, using an
     * associative accumulation
     * function, and returns an {@code OptionalLong} describing the reduced value,
     * if any. This is equivalent to:
     * <pre>{@code
     *     boolean foundAny = false;
     *     long result = null;
     *     for (long element : this stream) {
     *         if (!foundAny) {
     *             foundAny = true;
     *             result = element;
     *         }
     *         else
     *             result = accumulator.applyAsLong(result, element);
     *     }
     *     return foundAny ? OptionalLong.of(result) : OptionalLong.empty();
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code accumulator} function must be an
     * associative function.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param op an associative,
     *           non-interfering,
     *           stateless
     *           function for combining two values
     * @return the result of the reduction
     * @see #reduce(long, LongBinaryOperator)
     */
    default OptionalLong reduce(Distributed.LongBinaryOperator op) {
        return reduce((LongBinaryOperator) op);
    }

    /**
     * Performs a mutable
     * reduction operation on the elements of this stream.  A mutable
     * reduction is one in which the reduced value is a mutable result container,
     * such as an {@code ArrayList}, and elements are incorporated by updating
     * the state of the result rather than by replacing the result.  This
     * produces a result equivalent to:
     * <pre>{@code
     *     R result = supplier.get();
     *     for (long element : this stream)
     *         accumulator.accept(result, element);
     *     return result;
     * }</pre>
     *
     * <p>Like {@link #reduce(long, LongBinaryOperator)}, {@code collect} operations
     * can be parallelized without requiring additional synchronization.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param <R> type of the result
     * @param supplier a function that creates a new result container. For a
     *                 parallel execution, this function may be called
     *                 multiple times and must return a fresh value each time.
     * @param accumulator an associative,
     *                    non-interfering,
     *                    stateless
     *                    function for incorporating an additional element into a result
     * @param combiner an associative,
     *                    non-interfering,
     *                    stateless
     *                    function for combining two values, which must be
     *                    compatible with the accumulator function
     * @return the result of the reduction
     * @see Stream#collect(Supplier, BiConsumer, BiConsumer)
     */
    default <R> R collect(Distributed.Supplier<R> supplier, Distributed.ObjLongConsumer<R> accumulator,
                          Distributed.BiConsumer<R, R> combiner) {
        return collect((Supplier<R>) supplier, accumulator, combiner);
    }

    /**
     * Returns whether any elements of this stream match the provided
     * predicate.  May not evaluate the predicate on all elements if not
     * necessary for determining the result.  If the stream is empty then
     * {@code false} is returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if any elements of the stream match the provided
     * predicate, otherwise {@code false}
     */
    default boolean anyMatch(Distributed.LongPredicate predicate) {
        return anyMatch((LongPredicate) predicate);
    }

    /**
     * Returns whether all elements of this stream match the provided predicate.
     * May not evaluate the predicate on all elements if not necessary for
     * determining the result.  If the stream is empty then {@code true} is
     * returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if either all elements of the stream match the
     * provided predicate or the stream is empty, otherwise {@code false}
     */
    default boolean allMatch(Distributed.LongPredicate predicate) {
        return allMatch((LongPredicate) predicate);
    }

    /**
     * Returns whether no elements of this stream match the provided predicate.
     * May not evaluate the predicate on all elements if not necessary for
     * determining the result.  If the stream is empty then {@code true} is
     * returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if either no elements of the stream match the
     * provided predicate or the stream is empty, otherwise {@code false}
     */
    default boolean noneMatch(Distributed.LongPredicate predicate) {
        return noneMatch((LongPredicate) predicate);
    }

    /**
     * Returns a {@code DoubleStream} consisting of the elements of this stream,
     * converted to {@code double}.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a {@code DoubleStream} consisting of the elements of this stream,
     * converted to {@code double}
     */
    @Override
    DistributedDoubleStream asDoubleStream();

    /**
     * Returns a {@code Stream} consisting of the elements of this stream,
     * each boxed to a {@code Long}.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a {@code Stream} consistent of the elements of this stream,
     * each boxed to {@code Long}
     */
    @Override
    DistributedStream<Long> boxed();

    /**
     * Returns an equivalent stream that is sequential.  May return
     * itself, either because the stream was already sequential, or because
     * the underlying stream state was modified to be sequential.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a sequential stream
     */
    @Override
    DistributedLongStream sequential();

    /**
     * Returns an equivalent stream that is parallel.  May return
     * itself, either because the stream was already parallel, or because
     * the underlying stream state was modified to be parallel.
     *
     * <p>This is an intermediate operation.
     *
     * @return a parallel stream
     */
    @Override
    DistributedLongStream parallel();

    @Override
    DistributedLongStream filter(LongPredicate predicate);

    @Override
    DistributedLongStream map(LongUnaryOperator mapper);

    @Override
    <U> DistributedStream<U> mapToObj(LongFunction<? extends U> mapper);

    @Override
    DistributedIntStream mapToInt(LongToIntFunction mapper);

    @Override
    DistributedDoubleStream mapToDouble(LongToDoubleFunction mapper);

    @Override
    DistributedLongStream flatMap(LongFunction<? extends LongStream> mapper);

    @Override
    DistributedLongStream peek(LongConsumer action);

    @Override
    long reduce(long identity, LongBinaryOperator op);

    @Override
    OptionalLong reduce(LongBinaryOperator op);

    @Override
    <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner);

    @Override
    boolean anyMatch(LongPredicate predicate);

    @Override
    boolean allMatch(LongPredicate predicate);

    @Override
    boolean noneMatch(LongPredicate predicate);

    /**
     * @param jobConfig Job configuration which will be used while executing underlying DAG
     * @return the new stream
     */
    DistributedLongStream configure(JobConfig jobConfig);
}
