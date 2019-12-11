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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.Observer;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class ObservableImpl<T> implements Observable<T> {

    private final CopyOnWriteArrayList<Observer<T>> observers = new CopyOnWriteArrayList<>();
    private final String name;
    private final JetInstance jet;
    private final ILogger logger;

    private long lastSequence = -1;

    public ObservableImpl(String name, JetInstance jet, ILogger logger) {
        this.name = name;
        this.jet = jet;
        this.logger = logger;
        ObservableRepository.initObservable(name, this::onNewMessage, this::onSequenceNo, jet, logger);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void addObserver(@Nonnull Observer<T> observer) {
        observers.add(observer);
    }

    @Override
    public void addObserver(@Nonnull Consumer<? super T> onNext,
                            @Nonnull Consumer<? super Throwable> onError,
                            @Nonnull Runnable onComplete) {
        observers.add(Observer.of(onNext, onError, onComplete));
    }

    @Override
    public void destroy() {
        ObservableRepository.destroyObservable(name, jet.getHazelcastInstance());
    }

    public void onNewMessage(Object message) {
        if (message instanceof Throwable) {
            notifyObserversOfError((Throwable) message);
        } else if (message instanceof DoneItem) {
            notifyObserversOfEndOfData();
        } else {
            notifyObserversOfData(message);
        }
    }

    public void onSequenceNo(long sequence) {
        if (lastSequence > 0 && sequence > lastSequence + 1) {
            logger.warning(String.format("Observable '%s' lost %d internal messages", name, sequence - lastSequence + 1));
        }
        lastSequence = sequence;
    }

    @SuppressWarnings("unchecked")
    private void notifyObserversOfData(Object data) {
        T t = (T) data;
        for (Observer<T> observer : observers) {
            observer.onNext(t);
        }
    }

    private void notifyObserversOfEndOfData() {
        for (Observer<T> observer : observers) {
            observer.onComplete();
        }
    }

    private void notifyObserversOfError(Throwable throwable) {
        for (Observer<T> observer : observers) {
            observer.onError(throwable);
        }
    }

}
