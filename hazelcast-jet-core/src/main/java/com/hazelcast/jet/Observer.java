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

import java.util.function.Consumer;

/**
 * Watcher of the events produced by an {@link Observable}. Once subscribed
 * will be notified of all subsequent events produced by the
 * {@link Observable}.
 *
 * @param <T> type of data values in the sequence produced by the
 * {@link Observable}
 */
public interface Observer<T> {

    /**
     * Utility method for building an {@link Observer} from its basic
     * components.
     */
    static <T> Observer<T> of(Consumer<? super T> onNext, Consumer<? super Throwable> onError, Runnable onComplete) {
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
     * Method that will be called when the {@link Observable} (to which this
     * {@link Observer} is subscribed to) produces new data values.
     * <p>
     * The data values being passed via this method don't have a clear,
     * global ordering. Some {@link Observable}s for example produce values
     * in parallel so the order in which they arrive at subscribed
     * {@link Observer}s is unpredicable.
     * <p>
     * It is not possible to observe data values after an error or completion
     * has been observed.
     */
    void onNext(T t);

    /**
     * Method that will be called when the {@link Observable} (to which this
     * {@link Observer} is subscribed to) encounters an error in its
     * process of producing data values.
     * <p>
     * The passed {@link Throwable} instance attempts to reflect/explain the
     * original error as closely as possible.
     * <p>
     * Once an error has been observed no further data values nor completion
     * events will be received.
     */
    void onError(Throwable throwable);

    /**
     * Method that will be called when the {@link Observable} (to which this
     * {@link Observer} is subscribed to) finishes producing its sequence
     * of data values. Only batch-type of observables will ever produce
     * this event.
     * <p>
     * Once completion has been observed no further data values nor errors
     * will be received.
     */
    void onComplete();

}
