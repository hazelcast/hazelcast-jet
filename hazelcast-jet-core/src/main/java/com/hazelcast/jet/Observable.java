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

import javax.annotation.Nonnull;
import java.util.function.Consumer;

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
 *
 * @param <T> type of the values in the sequence
 */
public interface Observable<T> {

    /**
     * Returns the name identifying this particular observable.
     * @return name of observable
     */
    String name();

    /**
     * Register an instance of {@link Observer} to be notified about any
     * subsequent events (value updates, failures and completion).
     */
    void addObserver(@Nonnull Observer<T> observer);

    /**
     * Register explicit callbacks (fulfilling the purpose of an
     * {@link Observer}) to be notified about any subsequent events
     * (value updates, failures and completion).
     */
    void addObserver(@Nonnull Consumer<? super T> onNext,
                     @Nonnull Consumer<? super Throwable> onError,
                     @Nonnull Runnable onComplete);

    /**
     * Terminates this observable, including all remote objects used to
     * provide it.
     * <p>
     * The effect is incomplete if the observable is still being published
     * into (ie. the job populating it has not been completed). By incomplete
     * we mean that the flow of events will stop, but not all backing
     * remote objects will be cleaned up completely.
     */
    void destroy();

}
