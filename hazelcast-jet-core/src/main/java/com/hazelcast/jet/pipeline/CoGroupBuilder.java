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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.impl.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.PStreamImpl;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Javadoc pending.
 */
public class CoGroupBuilder<K, E_LEFT> {
    private final Map<TupleIndex<?>, CoGroupClause<?, K>> clauses = new HashMap<>();

    // Holds the TupleIndex of the "left-hand" component of the co-group
    // operation. This CoGroupClause instance is a special case which holds the
    // implied left-hand side of all co-group clauses.
    private final TupleIndex<E_LEFT> leftIndex;

    CoGroupBuilder(PStream<E_LEFT> leftStream, DistributedFunction<? super E_LEFT, K> leftKeyF) {
        this.leftIndex = add(leftStream, leftKeyF);
    }

    public TupleIndex<E_LEFT> leftIndex() {
        return leftIndex;
    }

    public <E> TupleIndex<E> add(PStream<E> s, DistributedFunction<? super E, K> groupKeyF) {
        TupleIndex<E> k = new TupleIndex<>(clauses.size());
        clauses.put(k, new CoGroupClause<>(s, groupKeyF));
        return k;
    }

    public PStream<KeyedTuple> build() {
        return new PStreamImpl<>(
                orderedClauses()
                       .map(e -> e.getValue().pstream())
                       .collect(toList()),
                new CoGroupTransform<>(orderedClauses()
                        .skip(1)
                        .map(e -> e.getValue().groupKeyF())
                        .collect(toList())),
                (PipelineImpl) clauses.get(leftIndex).pstream.getPipeline()
        );
    }

    private Stream<Entry<TupleIndex<?>, CoGroupClause<?, K>>> orderedClauses() {
        return clauses.entrySet().stream()
                      .sorted(comparing(Entry::getKey));
    }


    private static class CoGroupClause<E, K> {
        PStream<E> pstream;

        DistributedFunction<? super E, K> groupKeyF;

        CoGroupClause(PStream<E> pstream, DistributedFunction<? super E, K> groupKeyF) {
            this.pstream = pstream;
            this.groupKeyF = groupKeyF;
        }

        PStream<E> pstream() {
            return pstream;
        }

        DistributedFunction<? super E, K> groupKeyF() {
            return groupKeyF;
        }
    }
}
