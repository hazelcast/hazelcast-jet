/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.core.IList;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.DistributedDoubleStream;
import com.hazelcast.jet.stream.DistributedIntStream;
import com.hazelcast.jet.stream.DistributedLongStream;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.reducers.AnyMatchReducer;
import com.hazelcast.jet.stream.impl.reducers.BiConsumerCombinerReducer;
import com.hazelcast.jet.stream.impl.reducers.Reducers;
import com.hazelcast.jet.stream.impl.reducers.Reducers.BinaryAccumulateWithIdentity;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.stream.DistributedCollectors.toIList;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static com.hazelcast.util.Preconditions.checkTrue;

@SuppressWarnings(value = {"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
abstract class AbstractPipe<E_OUT> implements Pipe<E_OUT> {

    protected final StreamContext context;
    private final boolean isOrdered;

    AbstractPipe(StreamContext context) {
        this(context, false);
    }

    AbstractPipe(StreamContext context, boolean isOrdered) {
        this.context = context;
        this.isOrdered = isOrdered;
    }

    @Override
    public DistributedStream<E_OUT> filter(Predicate<? super E_OUT> predicate) {
        checkSerializable(predicate, "predicate");
        return new TransformPipe<>(context, this, t -> t.filter(predicate));
    }

    @Override
    public <R> DistributedStream<R> map(Function<? super E_OUT, ? extends R> mapper) {
        checkSerializable(mapper, "mapper");
        return new TransformPipe<>(context, this, t -> t.map(mapper));
    }

    @Override
    public DistributedIntStream mapToInt(ToIntFunction<? super E_OUT> mapper) {
        checkSerializable(mapper, "mapper");
        Pipe<Integer> map = (Pipe<Integer>) map(mapper::applyAsInt);
        return new IntPipeline(context, map);
    }

    @Override
    public DistributedLongStream mapToLong(ToLongFunction<? super E_OUT> mapper) {
        checkSerializable(mapper, "mapper");
        Pipe<Long> map = (Pipe<Long>) map(mapper::applyAsLong);
        return new LongPipe(context, map);
    }

    @Override
    public DistributedDoubleStream mapToDouble(ToDoubleFunction<? super E_OUT> mapper) {
        checkSerializable(mapper, "mapper");
        Pipe<Double> map = (Pipe<Double>) map(mapper::applyAsDouble);
        return new DoublePipeline(context, map);
    }

    @Override
    public <R> DistributedStream<R> flatMap(Function<? super E_OUT, ? extends Stream<? extends R>> mapper) {
        checkSerializable(mapper, "mapper");
        return new TransformPipe<>(context, this, t -> t.flatMap(item -> traverseStream(mapper.apply(item))));
    }

    @Override
    public DistributedIntStream flatMapToInt(Function<? super E_OUT, ? extends IntStream> mapper) {
        checkSerializable(mapper, "mapper");
        Pipe<Integer> pipeline = (Pipe<Integer>) flatMap(m -> mapper.apply(m).boxed());
        return new IntPipeline(context, pipeline);
    }

    @Override
    public DistributedLongStream flatMapToLong(Function<? super E_OUT, ? extends LongStream> mapper) {
        checkSerializable(mapper, "mapper");
        Pipe<Long> pipeline = (Pipe<Long>) flatMap(m -> mapper.apply(m).boxed());
        return new LongPipe(context, pipeline);
    }

    @Override
    public DistributedDoubleStream flatMapToDouble(Function<? super E_OUT, ? extends DoubleStream> mapper) {
        checkSerializable(mapper, "mapper");
        Pipe<Double> pipeline = (Pipe<Double>) flatMap(m -> mapper.apply(m).boxed());
        return new DoublePipeline(context, pipeline);
    }

    @Override
    public DistributedStream<E_OUT> distinct() {
        return new DistinctPipe<>(context, this);
    }

    @Override
    public DistributedStream<E_OUT> sorted() {
        return sorted(null);
    }

    @Override
    public final DistributedStream<E_OUT> sorted(Comparator<? super E_OUT> comparator) {
        checkSerializable(comparator, "comparator");
        return new SortPipe<>(this, context, comparator);
    }

    @Override
    public DistributedStream<E_OUT> peek(Consumer<? super E_OUT> action) {
        checkSerializable(action, "action");
        return new PeekPipe<>(context, this, action);
    }

    @Override
    public DistributedStream<E_OUT> limit(long maxSize) {
        return new LimitPipe<>(context, this, maxSize);
    }

    @Override
    public DistributedStream<E_OUT> skip(long n) {
        return new SkipPipe<>(context, this, n);
    }

    @Override
    public void forEach(Consumer<? super E_OUT> action) {
        IList<E_OUT> list = this.collect(toIList(uniqueListName()));
        list.forEach(action::accept);
        list.destroy();
    }

    @Override
    public void forEachOrdered(Consumer<? super E_OUT> action) {
        forEach(action);
    }

    @Override
    public Object[] toArray() {
        IList<E_OUT> list = collect(toIList(uniqueListName()));
        Object[] array = list.toArray();
        list.destroy();
        return array;
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        IList<E_OUT> list = collect(toIList(uniqueListName()));
        A[] array = generator.apply(list.size());
        array = list.toArray(array);
        list.destroy();
        return array;
    }

    @Override
    public E_OUT reduce(E_OUT identity, BinaryOperator<E_OUT> accumulator) {
        checkSerializable(identity, "identity");
        checkSerializable(accumulator, "accumulator");

        return collect(new BinaryAccumulateWithIdentity<>(identity, accumulator));
    }

    @Override
    public Optional<E_OUT> reduce(BinaryOperator<E_OUT> accumulator) {
        checkSerializable(accumulator, "accumulator");

        return collect(new Reducers.BinaryAccumulate<>(accumulator));
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super E_OUT, U> accumulator,
                        BinaryOperator<U> combiner) {
        checkSerializable(identity, "identity");
        checkSerializable(accumulator, "accumulator");
        checkSerializable(combiner, "combiner");

        return collect(new Reducers.AccumulateCombineWithIdentity<>(identity, accumulator, combiner));
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super E_OUT> accumulator,
                         BiConsumer<R, R> combiner) {
        checkSerializable(supplier, "supplier");
        checkSerializable(accumulator, "accumulator");
        checkSerializable(combiner, "combiner");
        return collect(new BiConsumerCombinerReducer<>(supplier, accumulator, combiner));
    }

    @Override
    public <R, A> R collect(Collector<? super E_OUT, A, R> collector) {
        checkTrue(collector instanceof DistributedCollector, "collector must of " +
                "type DistributedCollector");
        return collect((DistributedCollector<? super E_OUT, A, R>) collector);
    }

    @Override
    public <R> R collect(Reducer<? super E_OUT, R> reducer) {
        return reducer.reduce(context, this);
    }

    @Override
    public Optional<E_OUT> min(Comparator<? super E_OUT> comparator) {
        checkSerializable(comparator, "comparator");
        return reduce((left, right) -> comparator.compare(left, right) < 0 ? left : right);
    }

    @Override
    public Optional<E_OUT> max(Comparator<? super E_OUT> comparator) {
        checkSerializable(comparator, "comparator");
        return reduce((left, right) -> comparator.compare(left, right) > 0 ? left : right);
    }

    @Override
    public long count() {
        return reduce(0L,
                (i, m) -> i + 1,
                (a, b) -> a + b);
    }

    @Override
    public boolean anyMatch(Predicate<? super E_OUT> predicate) {
        checkSerializable(predicate, "predicate");

        return collect(new AnyMatchReducer<>(predicate));
    }

    @Override
    public boolean allMatch(Predicate<? super E_OUT> predicate) {
        return !anyMatch(t -> !predicate.test(t));
    }

    @Override
    public boolean noneMatch(Predicate<? super E_OUT> predicate) {
        return !anyMatch(predicate);
    }

    @Override
    public Optional<E_OUT> findFirst() {
        IList<E_OUT> first = this.limit(1).collect(toIList(uniqueListName()));
        Optional<E_OUT> value = first.size() == 0 ? Optional.empty() : Optional.of(first.get(0));
        first.destroy();
        return value;
    }

    @Override
    public Optional<E_OUT> findAny() {
        return findFirst();
    }

    @Override
    public Iterator<E_OUT> iterator() {
        IList<E_OUT> list = collect(toIList(uniqueListName()));
        Iterator<E_OUT> iterator = list.iterator();
        list.destroy();
        return iterator;
    }

    @Override
    public boolean isParallel() {
        return true;
    }

    @Override
    public DistributedStream<E_OUT> sequential() {
        throw new UnsupportedOperationException("Sequential streams are not supported for Hazelcast Jet");
    }

    @Override
    public DistributedStream<E_OUT> parallel() {
        return this;
    }

    @Override
    public DistributedStream<E_OUT> unordered() {
        if (isOrdered()) {
            return new UnorderedPipe<>(context, this);
        }
        return this;
    }

    @Override
    public DistributedStream<E_OUT> onClose(Runnable closeHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Jet streams are not closeable.");
    }

    /**
     * @return if this step in the pipeline is ordered
     */
    @Override
    public boolean isOrdered() {
        return isOrdered;
    }

    @Override
    public Spliterator<E_OUT> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DistributedStream<E_OUT> configure(JobConfig jobConfig) {
        context.setJobConfig(jobConfig);
        return this;
    }
}
