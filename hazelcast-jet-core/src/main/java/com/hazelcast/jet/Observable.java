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

import com.hazelcast.jet.impl.observer.BlockingIteratorObserver;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.UUID;

/**
 * Represents a flowing sequence of values. The sequence can be observed
 * by registering {@link Observer}s on it. Observers are able to see
 * all the values that show up in the sequence after they have been
 * registered.
 * <p>
 * Besides new values appearing observers can also observe completions and
 * failure events. Completion means that no further values will appear in
 * the sequence. Failure means that something went wrong during the
 * production of the sequence's values and the event attempts to provide
 * useful information about the cause of the problem.
 * <p>
 * Observable implementations are backed typically by long lived,
 * distributed data objects which get created when the {@link Observable}
 * is {@link JetInstance#getObservable(String) requested from a JetInstance}.
 * <p>
 * Their events are produced by {@link Job}s running
 * {@link com.hazelcast.jet.pipeline.Sinks#observable(String) observable
 * type sinks} and stay alive while their owner jobs keep running. When
 * these jobs are batched by nature and they complete (successfully
 * or with a failure) the observables owned by them remain alive for a
 * preconfigured timeout (see
 * {@link com.hazelcast.jet.core.JetProperties#JOB_RESULTS_TTL_SECONDS
 * JOB_RESULTS_TTL_SECONDS property}, defaults to 7 days) after which they
 * get cleaned up automatically. For streaming jobs no such automatic
 * clean-up is possible, since they never complete, unless they fail.
 * <p>
 * When using {@link Observable}s we encourage their manual cleanup, via
 * their {@link Observable#destroy()} method, whenever they are no longer
 * needed. Automatic clean-up shouldn't be relied upon. Even if a client
 * that has requested the {@link Observable} happens to crash, before
 * managing to do clean-up, it is still possible to manually destroy
 * the backing distributed structures by obtaining a new, identically named
 * {@link Observable} and calling {@link Observable#destroy()} on that.
 * <p>
 * It is possible to use the same {@link Observable} for multiple jobs, or
 * even use the same {@link Observable} multiple times in the same job (by
 * defining {@link com.hazelcast.jet.pipeline.Sinks#observable(String)
 * observable type sinks} with the same name). If done so one should be aware
 * that there will be parallel streams of events which can be intermingled
 * with eachother in all kinds of unexpected ways. It is not possible to
 * tell which events originate from which sink.
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
     * subsequent events (value updates, failures and completion).
     *
     * @return registration ID associated with the added {@link Observer},
     * can be used to remove the {@link Observer} later
     */
    UUID addObserver(@Nonnull Observer<T> observer);

    /**
     * Removes previously added {@link Observer}s, identified by their
     * assigned registration IDs. Removed {@link Observer}s will
     * not get notified about further observable events.
     */
    void removeObserver(UUID registrationId);

    /**
     * Non-thread safe iterable. The iterable returns an iterator
     * which can block when no additional data has available, but
     * the sequence is not completed yet.
     *
     * TODO: Proper contract specification
     *
     * @return
     */
    @Override
    @Nonnull
    default Iterator<T> iterator() {
        BlockingIteratorObserver<T> observer = new BlockingIteratorObserver<>();
        addObserver(observer);
        return observer;
    }

    /**
     * Removes all previously registered observers and attempts to terminate
     * all remote objects backing this particular observable.
     * <p>
     * If the observable is still being published into (ie. the job
     * populating it has not been completed), then the remote backing
     * objects will be recreated.
     */
    void destroy();

}
