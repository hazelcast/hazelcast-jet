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

import com.hazelcast.jet.pipeline.impl.JoinTransform;
import com.hazelcast.jet.pipeline.impl.PStreamImpl;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;

import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Javadoc pending.
 */
public class JoinBuilder<E_LEFT> {
    private final TupleIndex<E_LEFT> leftIndex;
    private final Map<TupleIndex<?>, JoinClause<?, E_LEFT, ?>> pstreams = new HashMap<>();

    JoinBuilder(PStream<E_LEFT> leftStream) {
        // The first tuple component is a special case that corresponds to
        // the left pstream, for which there is no JoinOn. This pstream is
        // the implied left-hand side of all join clauses.
        this.leftIndex = add(leftStream, null);
    }

    public <K, E_RIGHT> TupleIndex<E_RIGHT> add(PStream<E_RIGHT> s, JoinOn<K, E_LEFT, E_RIGHT> joinOn) {
        TupleIndex<E_RIGHT> ind = new TupleIndex<>(pstreams.size());
        pstreams.put(ind, new JoinClause<>(s, joinOn));
        return ind;
    }

    public PStream<KeyedTuple> build() {
        return new PStreamImpl<>(
                pstreams.values().stream()
                        .map(JoinClause::pstream)
                        .collect(toList()),
                new JoinTransform(pstreams.values().stream()
                                          .skip(1)
                                          .map(JoinClause::joinOn)
                                          .collect(toList())),
                (PipelineImpl) pstreams.get(leftIndex).pstream().getPipeline()
        );
    }

    private static class JoinClause<K, E_LEFT, E_RIGHT> {
        private final PStream<E_RIGHT> pstream;
        private final JoinOn<K, E_LEFT, E_RIGHT> joinOn;

        JoinClause(PStream<E_RIGHT> pstream, JoinOn<K, E_LEFT, E_RIGHT> joinOn) {
            this.pstream = pstream;
            this.joinOn = joinOn;
        }

        PStream<E_RIGHT> pstream() {
            return pstream;
        }

        JoinOn<K, E_LEFT, E_RIGHT> joinOn() {
            return joinOn;
        }
    }
}
