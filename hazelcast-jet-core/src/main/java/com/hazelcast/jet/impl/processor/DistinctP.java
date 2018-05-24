/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Batch processor that emits the distinct items it observes. Items are
 * distinct if they have non-equal keys as returned from the supplied
 * {@code keyFn}.
 * */
public class DistinctP<T, K> extends AbstractProcessor {
    private final Function<? super T, ? extends K> keyFn;
    private final Set<K> seenItems = new HashSet<>();
    private boolean readyForNewItem = true;

    public DistinctP(Function<? super T, ? extends K> keyFn) {
        this.keyFn = keyFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return readyForNewItem && !seenItems.add(keyFn.apply((T) item)) || (readyForNewItem = tryEmit(item));
    }
}
