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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.NULL_EMIT_POLICY;
import static com.hazelcast.jet.core.WatermarkGenerationParams.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientTimestampedSourceP;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Top-level class for Jet source builders. Refer to the builder
 * factory methods listed below.
 *
 * @see #batch(String, DistributedFunction)
 * @see #timestampedStream(String, DistributedFunction)
 * @see #stream(String, DistributedFunction)
 *
 * @param <S> type of the source state object
 */
public final class SourceBuilder<S> {
    // the m-prefix in field names is there to allow unqualified access from
    // inner classes while not clashing with parameter names. Parameter names
    // are public API.
    private final String mName;
    private final DistributedFunction<? super Processor.Context, ? extends S> mCreateFn;
    private DistributedConsumer<? super S> mDestroyFn;
    private int mPreferredLocalParallelism;

    /**
     * The buffer object that the {@code fillBufferFn} gets on each call. Used
     * in sources that emit items without a timestamp.
     *
     * @param <T> type of the buffer item
     */
    public interface SourceBuffer<T> {

        /**
         * Adds an item to the buffer.
         */
        void add(@Nonnull T item);

        /**
         * Closes the buffer, signaling that all items have been emitted.
         */
        void close();
    }

    /**
     * The buffer object that the {@code fillBufferFn} gets on each call. Used
     * in sources that emit timestamped items.
     *
     * @param <T> type of the buffer item
     */
    public interface TimestampedSourceBuffer<T> {

        /**
         * Adds an item to the buffer, assigning a timestamp to it. The timestamp
         * is in milliseconds.
         */
        void add(@Nonnull T item, long timestamp);

        /**
         * Closes the buffer, signaling that all items have been emitted.
         */
        void close();
    }

    private SourceBuilder(
            @Nonnull String name,
            @Nonnull DistributedFunction<? super Processor.Context, ? extends S> createFn
    ) {
        this.mName = name;
        this.mCreateFn = createFn;
    }

    /**
     * Returns a fluent-API builder with which you can create a {@linkplain
     * BatchSource batch source} for a Jet pipeline. The source will use
     * {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel worker that drives your source has its private instance of
     * a state object it gets from your {@code createFn}. To get the data items
     * to emit to the pipeline, the worker repeatedly calls your {@code
     * fillBufferFn} with the state object and a buffer object.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. It shouldn't add more than a thousand
     * items at once, but there is no limit enforced. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance. Once it has added all the data, the function must call
     * {@link SourceBuffer#close() buffer.close()}.
     * <p>
     * Unless you call {@link SourceBuilder.Batch#distributed(int) builder.distributed()},
     * Jet will create just a single worker that should emit all the data. If
     * you do call it, make sure your distributed source takes care of
     * splitting the data between workers. Your {@code createFn} should consult
     * {@link Processor.Context#totalParallelism() procContext.totalParallelism()}
     * and {@link Processor.Context#globalProcessorIndex() procContext.globalProcessorIndex()}.
     * Jet calls it exactly once with each {@code globalProcessorIndex} from 0
     * to {@code totalParallelism - 1} and each of the resulting state objects
     * must emit its unique slice of the total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * reads the lines from a single text file. Since you can't control on
     * which member of the Jet cluster the source's worker will run, the file
     * should be available on all members. The source emits one line per {@code
     * fillBufferFn} call.
     * <pre>{@code
     * BatchSource<String> fileSource = SourceBuilder
     *         .batch("file-source", x ->
     *             new BufferedReader(new FileReader("input.txt")))
     *         .<String>fillBufferFn((in, buf) -> {
     *             String line = in.readLine();
     *             if (line != null) {
     *                 buf.add(line);
     *             } else {
     *                 buf.close();
     *             }
     *         })
     *         .destroyFn(BufferedReader::close)
     *         .build();
     * Pipeline p = Pipeline.create();
     * BatchStage<String> srcStage = p.drawFrom(fileSource);
     * }</pre>
     *
     * @param name     a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the state object
     * @param <S>      type of the state object
     */
    @Nonnull
    public static <S> SourceBuilder<S>.Batch<Void> batch(
            @Nonnull String name, @Nonnull DistributedFunction<? super Processor.Context, ? extends S> createFn
    ) {
        return new SourceBuilder<S>(name, createFn).new Batch<Void>();
    }

    /**
     * Returns a fluent-API builder with which you can create an {@linkplain
     * StreamSource unbounded stream source} for a Jet pipeline. The source will
     * use {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel worker that drives your source has its private instance of
     * a state object it gets from your {@code createFn}. To get the data items
     * to emit to the pipeline, the worker repeatedly calls your {@code
     * fillBufferFn} with the state object and a buffer object.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. It shouldn't add more than a thousand
     * items at once, but there is no limit enforced. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Unless you call {@link SourceBuilder.Stream#distributed(int) builder.distributed()},
     * Jet will create just a single worker that should emit all the data. If
     * you do call it, make sure your distributed source takes care of
     * splitting the data between workers. Your {@code createFn} should consult
     * {@link Processor.Context#totalParallelism() procContext.totalParallelism()}
     * and {@link Processor.Context#globalProcessorIndex() procContext.globalProcessorIndex()}.
     * Jet calls it exactly once with each {@code globalProcessorIndex} from 0
     * to {@code totalParallelism - 1} and each of the resulting state objects
     * must emit its unique slice of the total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * reads lines of text from a TCP/IP socket. The source emits one line per
     * {@code fillBufferFn} call, or nothing if there's no data ready.
     * <pre>{@code
     * StreamSource<String> socketSource = SourceBuilder
     *         .stream("socket-source", ctx -> new BufferedReader(
     *                 new InputStreamReader(
     *                         new Socket("localhost", 7001).getInputStream())))
     *         .<String>fillBufferFn((in, buf) -> {
     *             if (in.ready()) {
     *                 buf.add(in.readLine());
     *             }
     *         })
     *         .destroyFn(BufferedReader::close)
     *         .build();
     * Pipeline p = Pipeline.create();
     * StreamStage<String> srcStage = p.drawFrom(fileSource);
     * }</pre>
     *
     * @param name     a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the state object
     * @param <S>      type of the state object
     */
    @Nonnull
    public static <S> SourceBuilder<S>.Stream<Void> stream(
            @Nonnull String name, @Nonnull DistributedFunction<? super Processor.Context, ? extends S> createFn
    ) {
        return new SourceBuilder<S>(name, createFn).new Stream<Void>();
    }

    /**
     * Returns a fluent-API builder with which you can create an {@linkplain
     * StreamSource unbounded stream source} for a Jet pipeline. The source
     * emits items that already have timestamps attached so you don't have to
     * call {@link StreamStage#addTimestamps} when constructing your pipeline.
     * It will use {@linkplain Processor#isCooperative() non-cooperative}
     * processors.
     * <p>
     * Each parallel worker that drives your source has its private instance of
     * a state object it gets from your {@code createFn}. To get the data items
     * to emit to the pipeline, the worker repeatedly calls your {@code
     * fillBufferFn} with the state object and a buffer object. The buffer's
     * {@link SourceBuilder.TimestampedSourceBuffer#add add()} method takes two
     * arguments: the item and the timestamp in milliseconds.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. It shouldn't add more than a thousand
     * items at once, but there is no limit enforced. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Unless you call {@link SourceBuilder.TimestampedStream#distributed(int)
     * builder.distributed()}, Jet will create just a single worker that should
     * emit all the data. If you do call it, make sure your distributed source
     * takes care of splitting the data between workers. Your {@code createFn}
     * should consult {@link Processor.Context#totalParallelism()
     * procContext.totalParallelism()} and {@link Processor.Context#globalProcessorIndex()
     * procContext.globalProcessorIndex()}. Jet calls it exactly once with each
     * {@code globalProcessorIndex} from 0 to {@code totalParallelism - 1} and
     * each of the resulting state objects must emit its unique slice of the
     * total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * reads lines of text from a TCP/IP socket, interpreting the first 9
     * characters as the timestamp. The source emits one item per
     * {@code fillBufferFn} call, or nothing if there's no data ready.
     * <pre>{@code
     * StreamSource<String> socketSource = SourceBuilder
     *     .timestampedStream("socket-source", ctx -> new BufferedReader(
     *         new InputStreamReader(
     *             new Socket("localhost", 7001).getInputStream())))
     *     .<String>fillBufferFn((in, buf) -> {
     *         if (in.ready()) {
     *             String line = in.readLine();
     *             long timestamp = Long.valueOf(line.substring(0, 9));
     *             buf.add(line.substring(9), timestamp);
     *         }
     *     })
     *     .destroyFn(BufferedReader::close)
     *     .build();
     * Pipeline p = Pipeline.create();
     * StreamStage<String> srcStage = p.drawFrom(fileSource);
     * }</pre>
     *
     * @param name a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the state object
     * @param <S> type of the state object
     */
    @Nonnull
    public static <S> SourceBuilder<S>.TimestampedStream<Void> timestampedStream(
            @Nonnull String name, @Nonnull DistributedFunction<? super Processor.Context, ? extends S> createFn
    ) {
        return new SourceBuilder<S>(name, createFn).new TimestampedStream<Void>();
    }

    private abstract class Base<T> {
        private Base() {
        }

        /**
         * Sets the function that Jet will call when cleaning up after a job has
         * ended. It gives you the opportunity to release any resources held by the
         * state object.
         */
        @Nonnull
        public Base<T> destroyFn(@Nonnull DistributedConsumer<? super S> destroyFn) {
            mDestroyFn = destroyFn;
            return this;
        }

        /**
         * Declares that you're creating a distributed source. On each member of
         * the cluster Jet will create as many workers as you specify with the
         * {@code preferredLocalParallelism} parameter. If you call this, you must
         * ensure that all the source workers are coordinated and not emitting
         * duplicated data. The {@code createFn} can consult {@link Processor.Context#totalParallelism()
         * procContext.totalParallelism()} and {@link Processor.Context#globalProcessorIndex()
         * procContext.globalProcessorIndex()}. Jet calls {@code createFn} exactly
         * once with each {@code globalProcessorIndex} from 0 to {@code
         * totalParallelism - 1}, this can help all the instances agree on which
         * part of the data to emit.
         *
         * @param preferredLocalParallelism requested number of workers on each cluster member
         */
        @Nonnull
        public Base<T> distributed(int preferredLocalParallelism) {
            checkPositive(preferredLocalParallelism, "Preferred local parallelism must be positive");
            mPreferredLocalParallelism = preferredLocalParallelism;
            return this;
        }
    }

    private abstract class BaseNoTimestamps<T> extends Base<T> {
        DistributedBiConsumer<? super S, ? super SourceBuffer<T>> fillBufferFn;

        private BaseNoTimestamps() {
        }

        /**
         * Sets the function that Jet will call whenever it needs more data from
         * your source. The function receives the state object obtained from
         * {@code createFn} and Jet's buffer object. It should add some items
         * to the buffer, ideally those it can produce without making any blocking
         * calls. On any given invocation the function may also choose not to add
         * any items. Jet will automatically employ an exponential backoff strategy
         * to avoid calling your function in a tight loop during a period when
         * there's no data ready to be emitted.
         *
         * @param fillBufferFn function that fills the buffer with source data
         * @param <T_NEW> type of the emitted items
         * @return this builder with the item type reset to the one inferred from
         *         {@code fillBufferFn}
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> BaseNoTimestamps<T_NEW> fillBufferFn(
                @Nonnull DistributedBiConsumer<? super S, ? super SourceBuffer<T_NEW>> fillBufferFn
        ) {
            BaseNoTimestamps<T_NEW> newThis = (BaseNoTimestamps<T_NEW>) this;
            newThis.fillBufferFn = fillBufferFn;
            return newThis;
        }
    }

    /**
     * A builder of a batch stream source.
     * @see SourceBuilder#batch(String, DistributedFunction)
     *
     * @param <T>
     */
    public final class Batch<T> extends BaseNoTimestamps<T> {
        private Batch() {
        }

        /**
         * {@inheritDoc}
         * <p>
         * Once it has emitted all the data, the function must call {@link
         * SourceBuffer#close}.
         */
        @Override @Nonnull
        public <T_NEW> SourceBuilder<S>.Batch<T_NEW> fillBufferFn(
                @Nonnull DistributedBiConsumer<? super S, ? super SourceBuffer<T_NEW>> fillBufferFn
        ) {
            return (Batch<T_NEW>) super.fillBufferFn(fillBufferFn);
        }

        @Override @Nonnull
        public Batch<T> destroyFn(@Nonnull DistributedConsumer<? super S> destroyFn) {
            return (Batch<T>) super.destroyFn(destroyFn);
        }

        @Override @Nonnull
        public Batch<T> distributed(int preferredLocalParallelism) {
            return (Batch<T>) super.distributed(preferredLocalParallelism);
        }

        /**
         * Builds and returns the batch source.
         */
        @Nonnull
        public BatchSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn must be non-null");
            return new BatchSourceTransform<>(mName,
                    convenientSourceP(mCreateFn, fillBufferFn, mDestroyFn, mPreferredLocalParallelism));
        }
    }

    /**
     * A builder of an unbounded stream source.
     * @see SourceBuilder#stream(String, DistributedFunction)
     *
     * @param <T>
     */
    public final class Stream<T> extends BaseNoTimestamps<T> {
        private Stream() {
        }

        @Override @Nonnull
        public <T_NEW> Stream<T_NEW> fillBufferFn(
                @Nonnull DistributedBiConsumer<? super S, ? super SourceBuffer<T_NEW>> fillBufferFn
        ) {
            return (Stream<T_NEW>) super.fillBufferFn(fillBufferFn);
        }

        @Override @Nonnull
        public Stream<T> destroyFn(@Nonnull DistributedConsumer<? super S> pDestroyFn) {
            return (Stream<T>) super.destroyFn(pDestroyFn);
        }

        @Override @Nonnull
        public Stream<T> distributed(int preferredLocalParallelism) {
            return (Stream<T>) super.distributed(preferredLocalParallelism);
        }

        /**
         * Builds and returns the unbounded stream source.
         */
        @Nonnull
        public StreamSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn must be non-null");
            return new StreamSourceTransform<>(
                    mName,
                    wmParams -> convenientSourceP(mCreateFn, fillBufferFn, mDestroyFn, mPreferredLocalParallelism),
                    false);
        }
    }

    /**
     * A builder of an unbounded stream source with timestamps.
     * @see SourceBuilder#timestampedStream(String, DistributedFunction)
     *
     * @param <T>
     */
    public final class TimestampedStream<T> extends Base<T> {
        private DistributedBiConsumer<? super S, ? super TimestampedSourceBuffer<T>> fillBufferFn;
        private long maxLag;

        private TimestampedStream() {
        }

        /**
         * Sets the function that Jet will call whenever it needs more data from
         * your source. The function receives the state object obtained from
         * {@code createFn} and Jet's buffer object. It should add some items
         * to the buffer, ideally those it can produce without making any blocking
         * calls. The buffer's {@link SourceBuilder.TimestampedSourceBuffer#add add()}
         * method takes two arguments: the item and the timestamp in milliseconds.
         * <p>
         * On any given invocation the function may also choose not to add
         * any items. Jet will automatically employ an exponential backoff strategy
         * to avoid calling your function in a tight loop during a period when
         * there's no data ready to be emitted.
         *
         * @param fillBufferFn function that fills the buffer with source data
         * @param <T_NEW> type of the emitted items
         * @return this builder with the item type reset to the one inferred from
         *         {@code fillBufferFn}
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> TimestampedStream<T_NEW> fillBufferFn(
                @Nonnull DistributedBiConsumer<? super S, ? super TimestampedSourceBuffer<T_NEW>> fillBufferFn
        ) {
            TimestampedStream<T_NEW> newThis = (TimestampedStream<T_NEW>) this;
            newThis.fillBufferFn = fillBufferFn;
            return newThis;
        }

        @Override @Nonnull
        public TimestampedStream<T> destroyFn(@Nonnull DistributedConsumer<? super S> pDestroyFn) {
            return (TimestampedStream<T>) super.destroyFn(pDestroyFn);
        }

        @Override @Nonnull
        public TimestampedStream<T> distributed(int preferredLocalParallelism) {
            return (TimestampedStream<T>) super.distributed(preferredLocalParallelism);
        }

        /**
         * Sets the limit on the amount of disorder (skew) present in the
         * timestamps of the events this source will emit. Any given event's
         * timestamp must be at most this many milliseconds behind the highest
         * timestamp emitted so far, otherwise it will be considered late and
         * dropped.
         *
         * @param allowedLateness limit on how much the timestamp of an event being emitted can lag behind
         *                        the highest emitted timestamp so far
         */
        @Nonnull
        public TimestampedStream<T> allowedLateness(int allowedLateness) {
            this.maxLag = allowedLateness;
            return this;
        }

        /**
         * Builds and returns the timestamped stream source.
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public StreamSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn must be set");
            StreamSourceTransform<JetEvent<T>> source = new StreamSourceTransform<>(
                    mName,
                    wmGenParams(
                            JetEvent::timestamp,
                            (e, timestamp) -> e,
                            limitingLag(maxLag),
                            NULL_EMIT_POLICY,
                            DEFAULT_IDLE_TIMEOUT
                    ),
                    wmParams -> convenientTimestampedSourceP(
                            mCreateFn, fillBufferFn, wmParams, mDestroyFn, mPreferredLocalParallelism),
                    true);
            return (StreamSource<T>) source;
        }
    }
}
