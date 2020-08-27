/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class SortPrepareP<V> extends AbstractProcessor {
    private final SortedSet<V> set;
    private ResultTraverser resultTraverser;

    public SortPrepareP(@Nullable Comparator<V> comparator) {
        this.set = new TreeSet<>(comparator);
    }

    protected boolean tryProcess0(@Nonnull Object item) {
        set.add((V) item);
        return true;
    }

    @Override
    public boolean complete() {
        if (resultTraverser == null) {
            resultTraverser = new ResultTraverser();
        }
        return emitFromTraverser(resultTraverser);
    }

    private class ResultTraverser implements Traverser<V> {
        private final Iterator<V> iterator = set.iterator();

        @Override
        public V next() {
            if (!iterator.hasNext()) {
                return null;
            }
            try {
                return iterator.next();
            } finally {
                iterator.remove();
            }
        }
    }
}
