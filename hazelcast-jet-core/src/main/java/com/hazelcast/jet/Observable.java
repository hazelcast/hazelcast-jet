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
import com.hazelcast.ringbuffer.Ringbuffer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a flowing sequence of events produced by jobs containing
 * {@link com.hazelcast.jet.pipeline.Sinks#observable(String)
 * observable type sinks}. The actual transport of the events is handled by
 * a {@link Ringbuffer}, this is where the above mentioned sinks publish
 * into. These {@link Ringbuffer}s get created when the {@link Observable}
 * is {@link JetInstance#getObservable(String) requested from a JetInstance}.
 * <p>
 * Observing the sequence on the client side can be accomplished by
 * registering {@link Observer}s on the {@link Observable}. Observers are
 * based on {@link Ringbuffer} listeners so they are able to see not just
 * the events that have happened after their registration, but past events
 * which happen to still reside in the {@link Ringbuffer} (ie. have not been
 * overwritten yet).
 * <p>
 * Besides new values appearing observers can also observe completion and
 * failure events. Completion means that no further values will appear in
 * the sequence. Failure means that something went wrong during the
 * production of the sequence's values and the event attempts to provide
 * useful information about the cause of the problem.
 * <p>
 * As stated before events are being produced by jobs running
 * {@link com.hazelcast.jet.pipeline.Sinks#observable(String) observable
 * type sinks}. When these jobs are batched by nature and they complete
 * (successfully or with a failure) the observables owned by them
 * (and implicitly the {@link Ringbuffer}s backing those in turn) remain
 * alive for a preconfigured time (see
 * {@link com.hazelcast.jet.core.JetProperties#JOB_RESULTS_TTL_SECONDS
 * JOB_RESULTS_TTL_SECONDS property}, defaults to 7 days) after which they
 * get cleaned up automatically. For streaming jobs no such automatic
 * clean-up is possible, since they never complete, unless they fail.
 * <p>
 * When using {@link Observable}s we encourage their manual cleanup, via
 * their {@link Observable#destroy()} method, whenever they are no longer
 * needed. Automatic clean-up shouldn't be relied upon. Even if a client
 * that has requested the {@link Observable} happens to crash before
 * managing to do clean-up, it is still possible to manually destroy
 * the backing {@link Ringbuffer}s by obtaining a new, identically named
 * {@link Observable} and calling {@link Observable#destroy()} on that.
 * <p>
 * It is possible to use the same {@link Observable} for multiple jobs, or
 * even use the same {@link Observable} multiple times in the same job (by
 * defining {@link com.hazelcast.jet.pipeline.Sinks#observable(String)
 * observable type sinks} with the same name). If done so one should be aware
 * that there will be parallel streams of events which can be intermingled
 * with eachother in all kinds of unexpected ways. It is not possible to
 * tell which events originate from which sink. Newly registered
 * {@link Observer}s seeing all events still in the {@link Ringbuffer}
 * further exacerbates this problem so we recommend that an
 * {@link Observable} with a certain name be used in a single sink of a
 * single job.
 *
 * @param <T> type of the values in the sequence
 */
public interface Observable<T> extends Iterable<T> {

    /**
     * Returns the name identifying this particular observable.
     * @return name of observable
     */
    String name();

    /**
     * Register an instance of {@link Observer} to be notified about any
     * future and past events (to the extent that these haven't yet been
     * overwritten in the backing {@link Ringbuffer}).
     *
     * @return registration ID associated with the added {@link Observer},
     * can be used to remove the {@link Observer} later
     */
    UUID addObserver(@Nonnull Observer<T> observer);

    /**
     * Removes previously added {@link Observer}s, identified by their
     * assigned registration IDs. Removed {@link Observer}s will
     * not get notified about further events.
     */
    void removeObserver(UUID registrationId);

    /**
     * Non-thread safe iterator that can block while waiting for additional
     * events.
     * <p>
     * Should be used only for {@link Observable}s populated by batch jobs,
     * because otherwise, without a completion event, iteration will never
     * end.
     * <p>
     * Is backed by a blocking list which stores all event until they
     * wait to be iterated over. Completion and error event also get put
     * into the list, thus they are serialized with data events.
     * Iteration will continue until either a completion or error event
     * is encountered.
     * <p>
     * When no further event are available, but neither completion nor
     * error has been encountered, the {@link Iterator#hasNext() hasNext()}
     * method will block.
     * <p>
     * When a failure event is encountered the {@link Iterator#hasNext()
     * hasNext()} call throws it as an Exception.
     */
    @Override
    @Nonnull
    default Iterator<T> iterator() {
        BlockingIteratorObserver<T> observer = new BlockingIteratorObserver<>();
        addObserver(observer);
        return observer;
    }

    /**
     * Future view of the observable sequence, on that can also apply a
     * transformation on the values. For example it could do counting,
     * like this: {@code observable.toFuture(Stream::count)}, but it's
     * also suited for mapping, filtering and so on.
     * <p>
     * Should be used only for {@link Observable}s populated by batch jobs,
     * because otherwise, without a completion event, the stream will never
     * end and the future will never be completed.
     *
     * @param fn transform function which takes the stream of observed values
     *           and produces an altered value from it, which could also
     *           be a stream
     */
    default <R> Future<R> toFuture(Function<Stream<T>, R> fn) {
        Iterator<T> iterator = iterator();
        return CompletableFuture.supplyAsync(() -> {
            Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
            return fn.apply(StreamSupport.stream(spliterator, false));
        });
    }

    /**
     * Removes all previously registered observers and attempts to destroy
     * the backing {@link Ringbuffer}.
     * <p>
     * If the {@link Ringbuffer} is still being published into (ie. the job
     * populating it has not been completed), then it will be recreated.
     */
    void destroy();

}
