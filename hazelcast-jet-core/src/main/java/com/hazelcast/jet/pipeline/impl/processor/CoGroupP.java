/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.tuple.Tuple2;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;

/**
 * Javadoc pending.
 */
@SuppressWarnings("unchecked")
public class CoGroupP<K, A, R> extends AbstractProcessor {
    private final List<DistributedFunction<?, ? extends K>> groupKeyFns;
    private final AggregateOperation<A, R> aggrOp;
    private final List<Tag> tags;
    private final Map<K, A> keyToAcc = new HashMap<>();

    public CoGroupP(List<DistributedFunction<?, ? extends K>> groupKeyFns,
                    AggregateOperation<A, R> aggrOp,
                    List<Tag> tags
    ) {
        this.groupKeyFns = groupKeyFns;
        this.aggrOp = aggrOp;
        this.tags = tags;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        System.out.println("CoGroupP recv #" + ordinal + ": " + item);
        Function<Object, ? extends K> keyF = (Function<Object, ? extends K>) groupKeyFns.get(ordinal);
        K key = keyF.apply(item);
        A acc = keyToAcc.computeIfAbsent(key, k -> aggrOp.createAccumulatorF().get());
        aggrOp.accumulateItemF(tags.get(ordinal))
              .accept(acc, item);
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverseIterable(keyToAcc.entrySet())
                .map(e -> new Tuple2<>(e.getKey(), aggrOp.finishAccumulationF().apply(e.getValue()))));
    }
}
