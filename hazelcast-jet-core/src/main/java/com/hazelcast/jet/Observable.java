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

import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.impl.observer.BlockingIteratorObserver;
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
 * com.hazelcast.jet.pipeline.Sinks#observable(String) observable sinks}.
 * To observe the events, call {@link #addObserver
 * jet.getObservable(name).addObserver(myObserver)}.
 * <p>
 * The identity of an {@code Observable} is its name. You can get any
 * number of {@code Observable} objects by the same name and they will
 * all correspond to the same entity. All registered observers will get
 * all the published events. Likewise, you can create any number of sinks
 * using the same name and they will all push the events to the same
 * {@code Observable}.
 * <p>
 * Even though an observable sink is the only way to publish data to an
 * {@code Observable}, their lifecycles are decoupled. You create an
 * {@code Observable} when you acquire it by name, either by running a
 * job with an observable sink or by calling {@link JetInstance#getObservable
 * jet.getObservable()}, and after that it stays alive until you explicitly
 * {@link #destroy} it, or until the auto-cleanup mechanism kicks in and
 * destroys it for you. (Keep in mind that a job may complete, but the
 * {@code Observable}, along with the data it published to it, lives on.
 * <strong>Consuming the data does not remove it from the
 * Observable</strong>. Also, if you destroy an {@code Observable} that is
 * still in active use by a sink, it will silently re-create it the next
 * time it has data to push to it.)
 * <p>
 * Auto-cleanup of the observables is a timeout based mechanism. The timeout
 * duration defaults to a week and can be configured via the {@link
 * JetProperties#JOB_RESULTS_TTL_SECONDS} property. The timer starts for an
 * observable when the {@link Job} containing sinks for it completes
 * (successfully, in case of batch jobs or with an error, for any job).
 * If there are multiple jobs containing sinks for the same observable,
 * then each subsequent job completion resets the timer (if it hasn't finished
 * yet). If there is a timer running for an observable and a new job is
 * submitted with sinks for that particular observable, then the timer
 * gets cancelled.
 * <p>
 * Jet stores the {@code Observable}'s data in a {@link Ringbuffer} and
 * observers are backed by {@link Ringbuffer} listeners. This results in
 * the following data retention semantics: an {@code Observable} holds on
 * to all the published events until reaching the configured capacity and
 * then starts overwriting the old events with new ones. A freshly
 * registered observer will see all the data available in the {@code
 * Ringbuffer}, including events that were published before registration.
 * <p>
 * In addition to data events, the observer can also observe completion and
 * failure events. Completion means that no further values will appear in
 * the sequence. Failure means that something went wrong during the
 * production of the sequence's values and the event attempts to provide
 * useful information about the cause of the problem.
 * <p>
 * You should explicitly destroy the {@code Observable} after use. Even if
 * the client that originally created the {@code Observable} crashes, any
 * other client can obtain the same {@code Observable} and destroy it.
 * <p>
 * While it's technically possible to use the same {@code Observable} from
 * multiple sinks or even jobs, it is not the intended kind of usage. The
 * events hold no metadata on their origin so the client will observe
 * events from all sinks arbitrarily interleaved with no way to tell which
 * came from which sink or job.
 *
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
