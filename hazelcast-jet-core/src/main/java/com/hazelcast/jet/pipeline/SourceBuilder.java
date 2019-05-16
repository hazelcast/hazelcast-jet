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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientTimestampedSourceP;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Top-level class for Jet custom source builders. Refer to the factory
 * methods:
 * <ul>
 *     <li>{@link #batch(String, FunctionEx)}
 *     <li>{@link #timestampedStream(String, FunctionEx)}
 *     <li>{@link #stream(String, FunctionEx)}
 * </ul>
 *
 * @param <C> type of the context object
 */
public final class SourceBuilder<C> {
    private final String name;
    private final FunctionEx<? super Context, ? extends C> createFn;
    private FunctionEx<? super C, Object> createSnapshotFn;
    private BiConsumerEx<? super C, ? super List<Object>> restoreSnapshotFn;
    private ConsumerEx<? super C> destroyFn = ConsumerEx.noop();
    private int preferredLocalParallelism;

    /**
     * The buffer object that the {@code fillBufferFn} gets on each call. Used
     * in sources that emit items without a timestamp.
     *
     * @param <T> type of the emitted item
     */
    public interface SourceBuffer<T> {

        /**
         * Returns the number of items the buffer holds.
         */
        int size();

        /**
         * Closes the buffer, signaling that all items have been emitted. Only
         * {@linkplain #batch} sources are allowed to call this method.
         *
         * @throws JetException if the source is a streaming source
         */
        void close() throws JetException;

        /**
         * Adds an item to the buffer.
         */
        void add(@Nonnull T item);
    }

    /**
     * The buffer object that the {@code fillBufferFn} gets on each call. Used
     * in sources that emit timestamped items.
     *
     * @param <T> type of the emitted item
     */
    public interface TimestampedSourceBuffer<T> extends SourceBuffer<T> {

        /**
         * Adds an item to the buffer, assigning a timestamp to it. The timestamp
         * is in milliseconds.
         */
        void add(@Nonnull T item, long timestamp);

        /**
         * Adds an item to the buffer, assigning {@code System.currentTimeMillis()}
         * to it as the timestamp.
         */
        @Override
        default void add(@Nonnull T item) {
            add(item, System.currentTimeMillis());
        }
    }

    private SourceBuilder(
            @Nonnull String name,
            @Nonnull FunctionEx<? super Context, ? extends C> createFn
    ) {
        this.name = name;
        this.createFn = createFn;
    }

    /**
     * Returns a fluent-API builder with which you can create a {@linkplain
     * BatchSource batch source} for a Jet pipeline. The source will use
     * {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel processor that drives your source has its private instance
     * of a <i>context object</i> it gets from your {@code createFn}. To get
     * the data items to emit to the pipeline, the processor repeatedly calls
     * your {@code fillBufferFn} with the context object and a buffer object.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. A hundred items at a time is enough to
     * eliminate any per-call overheads within Jet. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Once it has emitted all the data, {@code fillBufferFn} must call {@link
     * SourceBuffer#close() buffer.close()}. This signals Jet to not call {@code
     * fillBufferFn} again and at some later point it will call the {@code
     * destroyFn} with the context object.
     * <p>
     * Unless you call {@link SourceBuilder.Batch#distributed(int) builder.distributed()},
     * Jet will create just a single processor that should emit all the data.
     * If you do call it, make sure your distributed source takes care of
     * splitting the data between processors. Your {@code createFn} should
     * consult {@link Context#totalParallelism() procContext.totalParallelism()}
     * and {@link Context#globalProcessorIndex() procContext.globalProcessorIndex()}.
     * Jet calls it exactly once with each {@code globalProcessorIndex} from 0
     * to {@code totalParallelism - 1} and each of the resulting context objects
     * must emit its distinct slice of the total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * reads the lines from a single text file. Since you can't control on
     * which member of the Jet cluster the source's processor will run, the
     * file should be available on all members. The source emits one line per
     * {@code fillBufferFn} call.
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
     * @param createFn a function that creates the source's context object
     * @param <C>      type of the context object
     */
    @Nonnull
    public static <C> SourceBuilder<C>.Batch<Void> batch(
            @Nonnull String name, @Nonnull FunctionEx<? super Processor.Context, ? extends C> createFn
    ) {
        return new SourceBuilder<C>(name, createFn).new Batch<Void>();
    }

    /**
     * Returns a fluent-API builder with which you can create an {@linkplain
     * StreamSource unbounded stream source} for a Jet pipeline. The source will
     * use {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel processor that drives your source has its private instance
     * of a <i>context object</i> it gets from your {@code createFn}. To get
     * the data items to emit to the pipeline, the processor repeatedly calls
     * your {@code fillBufferFn} with the state object and a buffer object.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. A hundred items at a time is enough to
     * eliminate any per-call overheads within Jet. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Unless you call {@link SourceBuilder.Stream#distributed(int) builder.distributed()},
     * Jet will create just a single processor that should emit all the data.
     * If you do call it, make sure your distributed source takes care of
     * splitting the data between processors. Your {@code createFn} should
     * consult {@link Context#totalParallelism() procContext.totalParallelism()}
     * and {@link Context#globalProcessorIndex() procContext.globalProcessorIndex()}.
     * Jet calls it exactly once with each {@code globalProcessorIndex} from 0
     * to {@code totalParallelism - 1} and each of the resulting context objects
     * must emit its distinct slice of the total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * polls an URL and emits all the lines it gets in the response:
     * <pre>{@code
     * StreamSource<String> socketSource = SourceBuilder
     *     .stream("http-source", ctx -> HttpClients.createDefault())
     *     .<String>fillBufferFn((httpc, buf) -> {
     *         new BufferedReader(new InputStreamReader(
     *             httpc.execute(new HttpGet("localhost:8008"))
     *                  .getEntity().getContent()))
     *             .lines()
     *             .forEach(buf::add);
     *     })
     *     .destroyFn(Closeable::close)
     *     .build();
     * Pipeline p = Pipeline.create();
     * StreamStage<String> srcStage = p.drawFrom(socketSource);
     * }</pre>
     * <p>
     * <strong>NOTE:</strong> the source you build with this builder is not
     * fault-tolerant. You shouldn't use it in jobs that require a processing
     * guarantee.
     *
     * @param name     a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the source's context object
     * @param <C>      type of the context object
     */
    @Nonnull
    public static <C> SourceBuilder<C>.Stream<Void> stream(
            @Nonnull String name, @Nonnull FunctionEx<? super Processor.Context, ? extends C> createFn
    ) {
        return new SourceBuilder<C>(name, createFn).new Stream<Void>();
    }

    /**
     * Returns a fluent-API builder with which you can create an {@linkplain
     * StreamSource unbounded stream source} for a Jet pipeline. The source can
     * emit items with native timestamps, which you can enable by calling
     * {@linkplain StreamSourceStage#withNativeTimestamps
     * withNativeTimestamps()}. It will use {@linkplain
     * Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel processor that drives your source has its private instance
     * of a <i>context object</i> it gets from the given {@code createFn}. To get
     * the data items to emit to the pipeline, the processor repeatedly calls
     * your {@code fillBufferFn} with the context object and a buffer object. The
     * buffer's {@link SourceBuilder.TimestampedSourceBuffer#add add()} method
     * takes two arguments: the item and the timestamp in milliseconds.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. A hundred items at a time is enough to
     * eliminate any per-call overheads within Jet. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Unless you call {@link SourceBuilder.TimestampedStream#distributed(int)
     * builder.distributed()}, Jet will create just a single processor that
     * should emit all the data. If you do call it, make sure your distributed
     * source takes care of splitting the data between processors. Your {@code
     * createFn} should consult {@link Context#totalParallelism()
     * procContext.totalParallelism()} and {@link Context#globalProcessorIndex()
     * procContext.globalProcessorIndex()}. Jet calls it exactly once with each
     * {@code globalProcessorIndex} from 0 to {@code totalParallelism - 1} and
     * each of the resulting context objects must emit its distinct slice of the
     * total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * polls a URL and emits all the lines it gets in the response,
     * interpreting the first 9 characters as the timestamp.
     * <pre>{@code
     * StreamSource<String> socketSource = SourceBuilder
     *     .timestampedStream("http-source", ctx -> HttpClients.createDefault())
     *     .<String>fillBufferFn((httpc, buf) -> {
     *         new BufferedReader(new InputStreamReader(
     *             httpc.execute(new HttpGet("localhost:8008"))
     *                  .getEntity().getContent()))
     *             .lines()
     *             .forEach(line -> {
     *                 long timestamp = Long.valueOf(line.substring(0, 9));
     *                 buf.add(line.substring(9), timestamp);
     *             });
     *     })
     *     .destroyFn(Closeable::close)
     *     .build();
     * Pipeline p = Pipeline.create();
     * StreamStage<String> srcStage = p.drawFrom(socketSource)
     *         .withNativeTimestamps(SECONDS.toMillis(5));
     * }</pre>
     * <p>
     * <strong>NOTE:</strong> if the data source you're adapting to Jet is
     * partitioned, you may run into issues with event skew between partitions
     * assigned to single parallel processor. The timestamp you get from one
     * partition may be significantly behind the timestamp you already got from
     * another partition. If the skew is more than the allowed lag you have
     * {@linkplain StreamSourceStage#withNativeTimestamps(long) configured},
     * you risk that the events will be late. Use a {@linkplain
     * Sources#streamFromProcessorWithWatermarks custom processor} if you need
     * to coalesce watermarks from multiple partitions.
     *
     * @param name a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the source's context object
     * @param <C> type of the context object
     */
    @Nonnull
    public static <C> SourceBuilder<C>.TimestampedStream<Void> timestampedStream(
            @Nonnull String name,
            @Nonnull FunctionEx<? super Processor.Context, ? extends C> createFn
    ) {
        return new SourceBuilder<C>(name, createFn).new TimestampedStream<Void>();
    }

    private abstract class Base<T> {
        private Base() {
        }

        /**
         * Sets the function that Jet will call when cleaning up after an execution has
         * ended. It gives you the opportunity to release any resources held by the
         * context object. This function is also called when the job is cancelled or
         * restarted.
         */
        @Nonnull
        public Base<T> destroyFn(@Nonnull ConsumerEx<? super C> destroyFn) {
            SourceBuilder.this.destroyFn = destroyFn;
            return this;
        }

        /**
         * Declares that you're creating a distributed source. On each member of
         * the cluster Jet will create as many processors as you specify with the
         * {@code preferredLocalParallelism} parameter. If you call this, you must
         * ensure that all the source processors are coordinated and not emitting
         * duplicated data. The {@code createFn} can consult {@link Processor.Context#totalParallelism()
         * procContext.totalParallelism()} and {@link Processor.Context#globalProcessorIndex()
         * procContext.globalProcessorIndex()}. Jet calls {@code createFn} exactly
         * once with each {@code globalProcessorIndex} from 0 to {@code
         * totalParallelism - 1}, this can help all the instances agree on which
         * part of the data to emit.
         * <p>
         * If you don't call this method, there will be only one processor instance
         * running on an arbitrary member.
         *
         * @param preferredLocalParallelism requested number of processors on each cluster member
         */
        @Nonnull
        public Base<T> distributed(int preferredLocalParallelism) {
            checkPositive(preferredLocalParallelism, "Preferred local parallelism must >= 1");
            SourceBuilder.this.preferredLocalParallelism = preferredLocalParallelism;
            return this;
        }

        /**
         * Sets the function that Jet will call when it needs to save the
         * context's state to the snapshot, if the job has a {@linkplain
         * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
         * guarantee} set. Jet will call the function once per snapshot. Later,
         * if the job needs to restart from the snapshot, the returned object
         * will be passed to {@link
         * FaultTolerant#restoreSnapshotFn(BiConsumerEx) restoreSnapshotFn()}.
         * It can be any serializable object.
         *
         * <p>The function is allowed to return {@code null} to save no state.
         * In this case the {@code restoreSnapshotFn()} will not be called when
         * the job is restarted.
         *
         * <p>Example of a fault-tolerant generator of an infinite sequence of
         * integers (well, it will overflow at 2^31):
         *
         * <pre>{@code
         * StreamSource<Integer> source = SourceBuilder
         *         .stream("name", procCtx -> new MutableInteger())
         *         .<Integer>fillBufferFn((ctx, buffer) -> {
         *             for (int i = 0; i < 100; i++) {
         *                 buffer.add(ctx.getAndInc());
         *             }
         *         })
         *         .createSnapshotFn(ctx -> ctx.value)
         *         .restoreSnapshotFn((ctx, states) -> ctx.value = states.get(0))
         *         .build();
         * }</pre>
         *
         * @param createSnapshotFn a function to create state snapshot of the
         *                        context
         * @param <S> type of the object saved to state snapshot
         */
        @Nonnull
        <S> FaultTolerant<S, ? extends Base<T>> createSnapshotFn(
                @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn
        ) {
            return new FaultTolerant<>(this, createSnapshotFn);
        }
    }

    private abstract class BaseNoTimestamps<T> extends Base<T> {
        BiConsumerEx<? super C, ? super SourceBuffer<T>> fillBufferFn;

        private BaseNoTimestamps() {
        }

        /**
         * Sets the function that Jet will call whenever it needs more data from
         * your source. The function receives the context object obtained from
         * {@code createFn} and Jet's buffer object. It should add some items
         * to the buffer, ideally those it can produce without making any blocking
         * calls. On any given invocation the function may also choose not to add
         * any items. Jet will automatically employ an exponential backoff strategy
         * to avoid calling your function in a tight loop, if the previous call didn't
         * add any items to the buffer.
         *
         * @param fillBufferFn function that fills the buffer with source data
         * @param <T_NEW> type of the emitted items
         * @return this builder with the item type reset to the one inferred from
         *         {@code fillBufferFn}
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> BaseNoTimestamps<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuffer<T_NEW>> fillBufferFn
        ) {
            BaseNoTimestamps<T_NEW> newThis = (BaseNoTimestamps<T_NEW>) this;
            newThis.fillBufferFn = fillBufferFn;
            return newThis;
        }
    }

    /**
     * See {@link SourceBuilder#batch(String, FunctionEx)}.
     *
     * @param <T> type of emitted objects
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
        public <T_NEW> SourceBuilder<C>.Batch<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuffer<T_NEW>> fillBufferFn
        ) {
            return (Batch<T_NEW>) super.fillBufferFn(fillBufferFn);
        }

        @Override @Nonnull
        public Batch<T> destroyFn(@Nonnull ConsumerEx<? super C> destroyFn) {
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
            return new BatchSourceTransform<>(name, convenientSourceP(createFn, fillBufferFn, createSnapshotFn,
                    restoreSnapshotFn, destroyFn, preferredLocalParallelism, true));
        }
    }

    /**
     * See {@link SourceBuilder#stream(String, FunctionEx)}.
     *
     * @param <T> type of emitted objects
     */
    public final class Stream<T> extends BaseNoTimestamps<T> {
        private Stream() {
        }

        @Override @Nonnull
        public <T_NEW> Stream<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuffer<T_NEW>> fillBufferFn
        ) {
            return (Stream<T_NEW>) super.fillBufferFn(fillBufferFn);
        }

        @Override @Nonnull
        public Stream<T> destroyFn(@Nonnull ConsumerEx<? super C> pDestroyFn) {
            return (Stream<T>) super.destroyFn(pDestroyFn);
        }

        @Override @Nonnull
        public Stream<T> distributed(int preferredLocalParallelism) {
            return (Stream<T>) super.distributed(preferredLocalParallelism);
        }

        @SuppressWarnings("unchecked")
        @Override @Nonnull
        public <S> FaultTolerant<S, Stream<T>> createSnapshotFn(
                @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn
        ) {
            return (FaultTolerant<S, Stream<T>>) super.createSnapshotFn(createSnapshotFn);
        }

        /**
         * Builds and returns the unbounded stream source.
         */
        @Nonnull
        public StreamSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn() wasn't called");
            return new StreamSourceTransform<>(
                    name, eventTimePolicy -> convenientSourceP(createFn, fillBufferFn, createSnapshotFn, restoreSnapshotFn,
                    destroyFn, preferredLocalParallelism, false),
                    false, false);
        }
    }

    /**
     * See {@link SourceBuilder#timestampedStream(String, FunctionEx)}.
     *
     * @param <T> type of emitted objects
     */
    public final class TimestampedStream<T> extends Base<T> {
        private BiConsumerEx<? super C, ? super TimestampedSourceBuffer<T>> fillBufferFn;

        private TimestampedStream() {
        }

        /**
         * Sets the function that Jet will call whenever it needs more data from
         * your source. The function receives the context object obtained from
         * {@code createFn} and Jet's buffer object. It should add some items
         * to the buffer, ideally those it can produce without making any blocking
         * calls. The buffer's {@link SourceBuilder.TimestampedSourceBuffer#add add()}
         * method takes two arguments: the item and the timestamp in milliseconds.
         * <p>
         * On any given invocation the function may also choose not to add
         * any items. Jet will automatically employ an exponential backoff strategy
         * to avoid calling your function in a tight loop, if the previous call didn't
         * add any items to the buffer.
         *
         * @param fillBufferFn function that fills the buffer with source data
         * @param <T_NEW> type of the emitted items
         * @return this builder with the item type reset to the one inferred from
         *         {@code fillBufferFn}
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> TimestampedStream<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super TimestampedSourceBuffer<T_NEW>> fillBufferFn
        ) {
            TimestampedStream<T_NEW> newThis = (TimestampedStream<T_NEW>) this;
            newThis.fillBufferFn = fillBufferFn;
            return newThis;
        }

        @Override @Nonnull
        public TimestampedStream<T> destroyFn(@Nonnull ConsumerEx<? super C> pDestroyFn) {
            return (TimestampedStream<T>) super.destroyFn(pDestroyFn);
        }

        @Override @Nonnull
        public TimestampedStream<T> distributed(int preferredLocalParallelism) {
            return (TimestampedStream<T>) super.distributed(preferredLocalParallelism);
        }

        @SuppressWarnings("unchecked")
        @Override @Nonnull
        public <S> FaultTolerant<S, TimestampedStream<T>> createSnapshotFn(
                @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn
        ) {
            return (FaultTolerant<S, TimestampedStream<T>>) super.createSnapshotFn(createSnapshotFn);
        }

        /**
         * Builds and returns the timestamped stream source.
         */
        @Nonnull
        public StreamSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn must be set");
            return new StreamSourceTransform<>(
                    name,
                    eventTimePolicy -> convenientTimestampedSourceP(createFn, fillBufferFn, eventTimePolicy,
                            createSnapshotFn, restoreSnapshotFn, destroyFn, preferredLocalParallelism),
                    true, true);
        }
    }

    /**
     * A sub-builder to add the {@link #restoreSnapshotFn} after a {@link
     * Base#createSnapshotFn} was added.
     *
     * @param <S> type of the object saved to state snapshot
     * @param <B> type of the builder this sub-builder was created from
     */
    public final class FaultTolerant<S, B> {
        private final B parentBuilder;

        @SuppressWarnings("unchecked")
        private FaultTolerant(B parentBuilder, FunctionEx<? super C, ? extends S> createSnapshotFn) {
            this.parentBuilder = parentBuilder;
            SourceBuilder.this.createSnapshotFn = (FunctionEx<? super C, Object>) createSnapshotFn;
        }

        /**
         * Sets the function that Jet will call if it needs to restore the
         * context state from a snapshot. The function will be called once,
         * before the {@code fillBufferFn} was ever called.
         *
         * <p>If the source was not distributed, the list will contain exactly
         * one element. If it was, the function will receive a list of all
         * state objects saved by all parallel instances of the source. In that
         * case it needs to use only the part of all state objects that
         * pertains to each instance, see {@link Base#distributed(int)}.
         *
         * <p>The list of state objects won't contain possible nulls returned
         * by the {@link Base#createSnapshotFn(FunctionEx)} and will never be
         * empty.
         *
         * @param restoreSnapshotFn a function to apply saved state object to
         *                         the source context
         */
        @SuppressWarnings("unchecked")
        @Nonnull
        public B restoreSnapshotFn(@Nonnull BiConsumerEx<? super C, ? super List<S>> restoreSnapshotFn) {
            SourceBuilder.this.restoreSnapshotFn = (BiConsumerEx<? super C, ? super List<Object>>) restoreSnapshotFn;
            return parentBuilder;
        }
    }
}
