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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.Observer;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class ObservableImpl<T> implements Observable<T>, MessageListener<ObservableBatch> {

    private final CopyOnWriteArrayList<Observer<T>> observers = new CopyOnWriteArrayList<>();

    public ObservableImpl(ITopic<ObservableBatch> topic) {
        topic.addMessageListener(this);
    }

    @Override
    public void addObserver(@Nonnull Observer<T> observer) {
        this.observers.add(observer);
    }

    @Override
    public void onMessage(Message<ObservableBatch> message) {
        ObservableBatch batch = message.getMessageObject();

        Throwable throwable = batch.getThrowable();
        if (throwable != null) {
            notifyObserversOfError(throwable);
        } else {
            Object[] items = batch.getItems();
            if (items != null) {
                notifyObserversOfData(items);
            }

            if (batch.isEndOfData()) {
                notifyObserversOfEndOfData();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void notifyObserversOfData(Object[] items) {
        for (Object item : items) {
            T t = (T) item;
            for (Observer<T> observer : observers) {
                observer.onNext(t);
            }
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

    //todo: topics of observables should be cleaned up just as JobResults are... how does that work?

    public static ConsumerEx<ArrayList<Object>> observableItemsConsumer(HazelcastInstance instance, String name) {
        ITopic<ObservableBatch> topic = instance.getTopic(name);
        return buffer -> {
            topic.publish(ObservableBatch.items(buffer));
            buffer.clear();
        };
    }

    public static void notifyObservablesOfCompletion(HazelcastInstance instance, Set<String> observables,
                                                     Throwable error) {
        ObservableBatch batch = error == null ? ObservableBatch.endOfData() : ObservableBatch.error(error);
        for (String observable : observables) {
            ITopic<ObservableBatch> topic = instance.getTopic(observable);
            topic.publish(batch);
        }
    }
}
