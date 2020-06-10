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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.rocksdb.RocksDBStateBackend;
import com.hazelcast.jet.rocksdb.RocksMap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.function.Function;


/**
 * Implements the "collector" stage in a hash join transformation. This
 * stage collects the entire joined stream into a hashtable and then
 * broadcasts it to all local second-stage processors.
 */
public class HashJoinCollectP<K, T, V> extends AbstractProcessor {

    // the value is either a V or a HashJoinArrayList (if multiple values for
    // the key were observed)

    @Nonnull private final Function<T, K> keyFn;
    @Nonnull private final Function<T, V> projectFn;
    private RocksMap<K, Object> lookupTable;
    private RocksDBStateBackend store;

    public HashJoinCollectP(@Nonnull Function<T, K> keyFn, @Nonnull Function<T, V> projectFn) {
        this.keyFn = keyFn;
        this.projectFn = projectFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        store = context.rocksDBStateBackend();
        lookupTable = store.getMap();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        K key = keyFn.apply(t);
        V value = projectFn.apply(t);
        Object oldValue = lookupTable.get(key);
        lookupTable.put(key, merge(oldValue, value));
        return true;
    }

    private Object merge(Object oldValue, Object newValue) {
        if (oldValue == null) {
            return newValue;
        }

        if (oldValue instanceof HashJoinArrayList) {
            ((HashJoinArrayList) oldValue).add(newValue);
            return oldValue;
        } else {
            HashJoinArrayList res = new HashJoinArrayList();
            res.add(oldValue);
            res.add(newValue);
            return res;
        }
    }

    @Override
    public boolean complete() {
        store.flush();
        return tryEmit(lookupTable);
    }

    // We need a custom ArrayList subclass because the user's V type could be
    // ArrayList and then the logic that relies on instanceof would break
    static final class HashJoinArrayList extends ArrayList<Object> {
        HashJoinArrayList() {
            super(2);
        }
    }
}
