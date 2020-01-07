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

package com.hazelcast.jet;

import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.impl.observer.BlockingIteratorObserver;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.ringbuffer.Ringbuffer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a flowing sequence of events produced by one or more {@link
 * Sinks#observable(String) observable sinks}.
 * To observe the events, call {@link #addObserver
 * jet.getObservable(name).addObserver(myObserver)}. The {@code Observable}
 * is backed by a {@link Ringbuffer} and has a fixed capacity for storing
 * messages. It supports reading by multiple subscribers, which will all
 * observe the same sequence of messages. A new subscriber will start
 * reading automatically from the oldest sequence available. Once the
 * capacity is full, the oldest messages will be overwritten as new ones
 * arrive.
 * <p>
 * TODO: how to configure capacity, what is the default one?
 * <p>
 * In addition to data events, the observer can also observe completion and
 * failure events. Completion means that no further values will appear in
 * the sequence. Failure means that something went wrong during the
 * production of the sequence's values and the event attempts to provide
 * useful information about the cause of the problem.
 * <p>
 * <strong>Lifecycle</strong>
 * <p>
 * The lifecycle of the {@code Observable} is decoupled from the lifecycle
 * of the job. The {@code Observable} is created either when the user
 * gets a reference to it through {@link JetInstance#getObservable(String)}
 * or when the sink starts writing to it.
 * <p>
 * The {@code Observable} must be explicitly destroyed when it's no longer
 * in use, or data will be retained in the cluster.
 * <p>
 * <strong>Important:</strong> The same {@code Observable} must
 * <strong>not</strong> be used again in a new job since this will cause
 * in completion events interleaving and causing data loss or other unexpected
 * behaviour.
 * <p>
 * @param <T> type of the values in the sequence
 *
 * @since 4.0
 */
public interface Observable<T> extends Iterable<T> {

    /**
     * Returns the name identifying this particular observable.
     *
     * @return name of observable
     */
    @Nonnull
    String name();

    /**
     * Registers an {@link Observer} to this {@code Observable}. It will
     * receive all events currently in the backing {@link Ringbuffer} and then
     * continue receiving any future events.
     *
     * @return registration ID associated with the added {@code Observer}, can be used
     *         to remove the {@code Observer} later
     */
    @Nonnull
    UUID addObserver(@Nonnull Observer<T> observer);

    /**
     * Removes a previously added {@link Observer} identified by its
     * assigned registration ID. A removed {@code Observer} will not get
     * notified about further events.
     */
    void removeObserver(@Nonnull UUID registrationId);

    /**
     * Returns an iterator over the sequence of events produced by this
     * {@code Observable}. If there are currently no events to observe,
     * the iterator's {@code Iterator#hasNext hasNext()} and {@code
     * Iterator#next next()} methods will block. A completion event
     * completes the iterator ({@code hasNext()} will return false) and
     * a failure event makes the iterator's methods throw the underlying
     * exception.
     * <p>
     * If used against an {@code Observable} populated from a streaming job,
     * the iterator will complete only in the case of an error or job
     * cancellation.
     * <p>
     * The iterator is not thread-safe.
     * <p>
     * The iterator is backed by a blocking concurrent queue which stores all
     * events until consumed.
     */
    @Nonnull @Override
    default Iterator<T> iterator() {
        BlockingIteratorObserver<T> observer = new BlockingIteratorObserver<>();
        addObserver(observer);
        return observer;
    }

    /**
     * Allows you to post-process the results of a Jet job on the client side
     * using the standard Java {@link java.util.stream Stream API}. You provide
     * a function that will receive the job results as a {@code Stream<T>} and
     * return a single result.
     * <p>
     * Returns a {@link CompletableFuture CompletableFuture<R>} that will become
     * completed once your function has received all the job results through
     * its {@code Stream} and returned the final result.
     * <p>
     * A trivial example is counting, like this: {@code observable.toFuture(Stream::count)},
     * however the Stream API is quite rich and you can perform arbitrary
     * transformations and aggregations.
     * <p>
     * This feature is intended to be used only on the results of a batch job.
     * On an unbounded streaming job the stream-collecting operation will never
     * reach the final result.
     *
     * @param fn transform function which takes the stream of observed values
     *           and produces an altered value from it, which could also
     *           be a stream
     */
    @Nonnull
    default <R> CompletableFuture<R> toFuture(@Nonnull Function<Stream<T>, R> fn) {
        Objects.requireNonNull(fn, "fn");

        Iterator<T> iterator = iterator();
        return CompletableFuture.supplyAsync(() -> {
            Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
            return fn.apply(StreamSupport.stream(spliterator, false));
        });
    }

    /**
     * Removes all previously registered observers and destroys the backing
     * {@link Ringbuffer}.
     * <p>
     * <strong>Note:</strong> if you call this while a job that publishes to this
     * {@code Observable} is still active, it will silently create a new {@code
     * Ringbuffer} and go on publishing to it.
     */
    void destroy();

}
