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

package com.hazelcast.jet.impl.util;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Non-blocking thread-safe collection returning a refreshable iterator. The collection is biased towards fast repeated
 * iteration over all stored elements.
 *
 * Some of the mutation methods are currently not implemented - they can be added as needed.
 *
 * @param <T>
 */
public final class CopyOnWriteCollection<T> implements Collection<T> {
    private static final AtomicReferenceFieldUpdater<CopyOnWriteCollection, Object[]> UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(CopyOnWriteCollection.class, Object[].class, "array");
    private volatile Object[] array;

    public CopyOnWriteCollection() {
        this.array = new Object[0];
    }

    @Override
    public int size() {
        return array.length;
    }

    @Override
    public boolean isEmpty() {
        return array.length == 0;
    }

    @Override
    public boolean contains(Object o) {
        Object[] elements = array;
        return indexOf(o, elements) != -1;
    }

    /**
     * @return {@link RefreshableIterator} which can be refreshed. Refresing means the iterator will point to the
     * current snapshot of the collection and its internal position is set to the 1st element. The refresh
     * operation is cheap and entirely garbage-free.
     *
     */
    @Override
    @Nonnull
    public RefreshableIterator iterator() {
        return new RefreshableIterator(array);
    }

    @Override
    @Nonnull
    public Object[] toArray() {
        Object[] elements = array;
        return Arrays.copyOf(elements, elements.length);
    }

    @Override
    @Nonnull
    public <TARGET_T> TARGET_T[] toArray(@Nonnull TARGET_T[] target) {
        Object[] elements = array;
        int elementLength = elements.length;
        if (target.length < elementLength) {
            return (TARGET_T[]) Arrays.copyOf(elements, elementLength, target.getClass());
        }
        System.arraycopy(elements, 0, target, 0, elementLength);
        if (target.length > elementLength) {
            target[elementLength] = null;
        }
        return target;
    }

    @Override
    public boolean add(T t) {
        Object[] elements;
        Object[] newElements;
        do {
            elements = array;
            int elementLength = elements.length;
            newElements = Arrays.copyOf(elements, elementLength + 1);
            newElements[elementLength] = t;
        } while (!UPDATER.compareAndSet(this, elements, newElements));
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        Object[] elements = array;
        for (Object o : c) {
            if (indexOf(o, elements) == -1) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        Object[] currentElements;
        Object[] newElements;

        Object[] objectsToBeAdded = c.toArray();
        do {
            currentElements = array;
            int currentSize = currentElements.length;
            int newSize = currentSize + objectsToBeAdded.length;
            newElements = Arrays.copyOf(currentElements, newSize);
            System.arraycopy(objectsToBeAdded, 0, newElements, currentSize, objectsToBeAdded.length);
        } while (!UPDATER.compareAndSet(this, currentElements, newElements));
        return true;
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean remove(Object o) {
        Object[] currentElements;
        Object[] newElements;
        do {
            currentElements = array;
            int currentSize = currentElements.length;
            int index = indexOf(o, currentElements);
            if (index == -1) {
                return false;
            }
            newElements = new Object[currentSize - 1];
            System.arraycopy(currentElements, 0, newElements, 0, index);
            System.arraycopy(currentElements, index + 1, newElements, index, currentSize - index - 1);
        } while (!UPDATER.compareAndSet(this, currentElements, newElements));
        return true;
    }

    @Override
    public void clear() {
        array = new Object[0];
    }

    private static int indexOf(Object o, Object[] elements) {
        if (o == null) {
            for (int i = 0; i < elements.length; i++) {
                Object element = elements[i];
                if (element == null) {
                    return i;
                }
            }
            return -1;
        }

        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            if (o.equals(element)) {
                return i;
            }
        }
        return -1;
    }

    public final class RefreshableIterator implements Iterator<T> {
        private Object[] arr;
        private int currentPos;

        private RefreshableIterator(Object[] arr) {
            this.arr = arr;
        }

        public void refresh() {
            arr = CopyOnWriteCollection.this.array;
            currentPos = 0;
        }

        @Override
        public boolean hasNext() {
            return arr.length > currentPos;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T object = (T) arr[currentPos];
            currentPos++;
            return object;
        }
    }
}

