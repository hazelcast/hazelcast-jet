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

import com.hazelcast.jet.Observer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * TODO: Proper contract
 *
 * Implementation notes/assumptions:
 * 1. onComplete() called after all onNext()/onError() completed
 * 2. Iterator is single-threaded
 * 3. both next() and hasNext() block when no item is available, but onComplete() has not been called
 *
 * @param <T>
 */
public final class BlockingIteratorObserver<T> implements Iterator<T>, Observer<T> {
    private static final Object COMPLETED = new Object();

    private final BlockingQueue<Object> itemQueue;
    private Object next;
    private volatile Throwable error;

    public BlockingIteratorObserver() {
        this.itemQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void onNext(@Nonnull T t) {
        assert next != COMPLETED;
        itemQueue.add(t);
    }

    @Override
    public void onError(@Nonnull Throwable throwable) {
        assert next != COMPLETED;
        error = throwable;
        itemQueue.add(COMPLETED);
    }

    @Override
    public void onComplete() {
        assert next != COMPLETED;
        itemQueue.add(COMPLETED);
    }


    @Override
    public boolean hasNext() {
        if (next == null) {
            advanceBlocking();
        }
        if (next != COMPLETED) {
            return true;
        }
        if (error != null) {
            throw rethrow(error);
        }
        return false;
    }

    private void advanceBlocking() {
        try {
            next = itemQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    @Override
    @Nonnull
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T item = (T) next;
        next = null;
        return item;
    }
}
