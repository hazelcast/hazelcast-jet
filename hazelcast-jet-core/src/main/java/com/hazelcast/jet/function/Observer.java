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

package com.hazelcast.jet.function;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Observable;

import javax.annotation.Nonnull;

/**
 * Watcher of the events produced by an {@link Observable}. Once subscribed, it
 * will be notified of all past events produced by the {@link Observable}
 * (to the extent to which these haven't yet been overwritten in the
 * {@link Observable} implementation's finite history) and
 * all future events that will be produced by the {@link Observable}, while
 * the {@link Observer} remains added to it.
 * <p>
 * Notification will be done by an internal pool of threads and care should
 * be taken to not block those threads, ie. finish processing of all events
 * as soon as possible.
 *
 * @param <T> type of data values in the sequence produced by the
 * {@link Observable}
 */
@FunctionalInterface
public interface Observer<T> {

    /**
     * Utility method for building an {@link Observer} from its basic
     * components.
     */
    static <T> Observer<T> of(
            @Nonnull ConsumerEx<? super T> onNext,
            @Nonnull ConsumerEx<? super Throwable> onError,
            @Nonnull RunnableEx onComplete
    ) {
        return new Observer<T>() {
            @Override
            public void onNext(T t) {
                onNext.accept(t);
            }

            @Override
            public void onError(Throwable throwable) {
                onError.accept(throwable);
            }

            @Override
            public void onComplete() {
                onComplete.run();
            }
        };
    }

    /**
     * Method that will be called when data values from the {@link Observable}
     * become available.
     * <p>
     * The data values passed via this method don't have a clear,
     * global ordering. Some {@link Observable}s for example produce values
     * in parallel so the order in which they arrive at the
     * {@link Observer}s is unpredictable.
     * <p>
     * It is not possible to observe data values after an error or completion
     * has been observed.
     */
    void onNext(@Nonnull T t);

    /**
     * Method that will be called when the {@link Observable} (to which this
     * {@link Observer} is subscribed to) encounters an error in its
     * process of producing data values.
     * <p>
     * The passed {@link Throwable} instance attempts to reflect/explain the
     * original error as closely as possible.
     * <p>
     * Once an error has been observed, no further data values nor completion
     * events will be received.
     */
    default void onError(@Nonnull Throwable throwable) {
        throwable.printStackTrace();
    }

    /**
     * Method that will be called when the {@link Observable} (to which this
     * {@link Observer} is subscribed to) finishes producing its sequence
     * of data values. Only batch-type of observables will ever produce
     * this event.
     * <p>
     * Once completion has been observed, no further data values nor errors
     * will be received.
     */
    default void onComplete() {
        //do nothing
    }

}
