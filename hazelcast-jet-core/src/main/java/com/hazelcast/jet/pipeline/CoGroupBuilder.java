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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.bag.Tag;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.bag.Tag.tag;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Javadoc pending.
 */
public class CoGroupBuilder<K, E0> {
    private final Map<Tag<?>, CoGroupClause<?, K>> clauses = new HashMap<>();
    private final List<Tag> tags = new ArrayList<>();

    CoGroupBuilder(ComputeStage<E0> s, DistributedFunction<? super E0, K> groupKeyF) {
        add(s, groupKeyF);
    }

    public Tag<E0> tag0() {
        return Tag.tag0();
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(ComputeStage<E> s, DistributedFunction<? super E, K> groupKeyF) {
        Tag tag = tag(clauses.size());
        clauses.put(tag, new CoGroupClause<>(s, groupKeyF));
        tags.add(tag);
        return (Tag<E>) tag;
    }

    @SuppressWarnings("unchecked")
    public <A, R> ComputeStage<Tuple2<K, R>> build(AggregateOperation<A, R> aggrOp) {
        List<ComputeStage> upstream = orderedClauses()
                .map(e -> e.getValue().pstream())
                .collect(toList());
        CoGroupTransform<K, A, R> transform = new CoGroupTransform<K, A, R>(orderedClauses()
                .map(e -> e.getValue().groupKeyF())
                .collect(toList()),
                aggrOp,
                tags
        );
        PipelineImpl pipeline = (PipelineImpl) clauses.get(tag0()).pstream.getPipeline();
        return pipeline.attach(upstream, transform);
    }

    private Stream<Entry<Tag<?>, CoGroupClause<?, K>>> orderedClauses() {
        return clauses.entrySet().stream()
                      .sorted(comparing(Entry::getKey));
    }

    private static class CoGroupClause<E, K> {
        private final ComputeStage<E> pstream;
        private final DistributedFunction<? super E, K> groupKeyF;

        CoGroupClause(ComputeStage<E> pstream, DistributedFunction<? super E, K> groupKeyF) {
            this.pstream = pstream;
            this.groupKeyF = groupKeyF;
        }

        ComputeStage<E> pstream() {
            return pstream;
        }

        DistributedFunction<? super E, K> groupKeyF() {
            return groupKeyF;
        }
    }
}
