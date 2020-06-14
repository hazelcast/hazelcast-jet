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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.rocksdb.RocksDBStateBackend;
import com.hazelcast.jet.rocksdb.RocksMap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.util.Collections.singletonList;

/**
 * Batch processor that groups items by key and computes the supplied
 * aggregate operation on each group. The items may originate from one or
 * more inbound edges. The supplied aggregate operation must have as many
 * accumulation functions as there are inbound edges.
 */
public class GroupP<K, A, R, OUT> extends AbstractProcessor {
    @Nonnull private final List<FunctionEx<?, ? extends K>> groupKeyFns;
    @Nonnull private final AggregateOperation<A, R> aggrOp;
    private final BiFunction<? super K, ? super R, OUT> mapToOutputFn;
    private Traverser<OUT> resultTraverser;
    private RocksMap<K, A> keyToAcc;
    private RocksMap<K, Entry<Integer, Object>> keyToValues;

    public GroupP(
            @Nonnull List<FunctionEx<?, ? extends K>> groupKeyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull BiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        checkTrue(groupKeyFns.size() == aggrOp.arity(), groupKeyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");
        this.groupKeyFns = groupKeyFns;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    public <T> GroupP(
            @Nonnull FunctionEx<? super T, ? extends K> groupKeyFn,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull BiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        this(singletonList(groupKeyFn), aggrOp, mapToOutputFn);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        RocksDBStateBackend store = context.rocksDBStateBackend();
        keyToAcc = store.getMap();
        keyToValues = store.getMap();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Function<Object, ? extends K> keyFn = (Function<Object, ? extends K>) groupKeyFns.get(ordinal);
        K key = keyFn.apply(item);
        keyToValues.prefixWrite(key, Tuple2.tuple2(ordinal, item));
        return true;
    }

    @Override
    public boolean complete() {
        if (resultTraverser == null) {
            resultTraverser = new ResultTraverser()
                    // reuse null filtering done by map()
                    .map(e -> mapToOutputFn.apply(e.getKey(), aggrOp.finishFn().apply(e.getValue())));
        }
        return emitFromTraverser(resultTraverser);
    }

    private class ResultTraverser implements Traverser<Entry<K, A>> {
        Iterator<Entry<K, Entry<Integer, Object>>> iterator = keyToValues.iterator();

        ResultTraverser() {
            keyToValues.compact();
        }

        @Override
        public Entry<K, A> next() {
            if (!iterator.hasNext()) {
                return null;
            }
            K key = iterator.next().getKey();
            A acc = aggrOp.createFn().get();
            Object result = keyToValues.prefixRead(keyToValues.prefixIterator(), key);
            if (result instanceof ArrayList) {
                ArrayList<Entry<Integer, Object>> values = (ArrayList<Entry<Integer, Object>>) result;
                for (Entry<Integer, Object> e : values) {
                    aggrOp.accumulateFn(e.getKey()).accept(acc, e.getValue());
                }
                //skip over current prefix
                for (int i = 0; i < values.size() - 1; i++) {
                    iterator.next();
                }
            } else {
            Entry<Integer, Object> value = (Entry<Integer, Object>) result;
            aggrOp.accumulateFn(value.getKey()).accept(acc, value.getValue());
        }
            return Tuple2.tuple2(key, acc);
    }
    }
}
