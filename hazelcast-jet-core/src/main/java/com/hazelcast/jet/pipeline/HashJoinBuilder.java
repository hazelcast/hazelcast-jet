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

import com.hazelcast.jet.pipeline.datamodel.Tag;
import com.hazelcast.jet.pipeline.datamodel.TaggedMap;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.datamodel.Tag.tag;
import static com.hazelcast.jet.pipeline.datamodel.Tag.tag0;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Builds a hash join transform with any number of joined streams.
 */
public class HashJoinBuilder<E0> {
    private final Map<Tag<?>, StageAndClause> clauses = new HashMap<>();

    HashJoinBuilder(ComputeStage<E0> leftStream) {
        add(leftStream, null);
    }

    public <K, E1_IN, E1> Tag<E1> add(ComputeStage<E1_IN> s, JoinClause<K, E0, E1_IN, E1> joinClause) {
        Tag<E1> tag = tag(clauses.size());
        clauses.put(tag, new StageAndClause<>(s, joinClause));
        return tag;
    }

    @SuppressWarnings("unchecked")
    public ComputeStage<Tuple2<E0, TaggedMap>> build() {
        List<Entry<Tag<?>, StageAndClause>> orderedClauses = clauses.entrySet().stream()
                                                                    .sorted(comparing(Entry::getKey))
                                                                    .collect(toList());
        List<ComputeStage> upstream = orderedClauses.stream()
                                                    .map(e -> e.getValue().stage())
                                                    .collect(toList());
        // A probable javac bug forced us to extract this variable
        Stream<JoinClause<?, E0, ?, ?>> joinClauses = orderedClauses
                .stream()
                .skip(1)
                .map(e -> e.getValue().clause());
        MultiTransform hashJoinTransform = Transforms.hashJoin(
                joinClauses.collect(toList()),
                orderedClauses.stream()
                              .skip(1)
                              .map(Entry::getKey)
                              .collect(toList()));
        PipelineImpl pipeline = (PipelineImpl) clauses.get(tag0()).stage().getPipeline();
        return pipeline.attach(upstream, hashJoinTransform);
    }

    private static class StageAndClause<K, E0, E1, E1_OUT> {
        private final ComputeStage<E1> stage;
        private final JoinClause<K, E0, E1, E1_OUT> joinClause;

        StageAndClause(ComputeStage<E1> stage, JoinClause<K, E0, E1, E1_OUT> joinClause) {
            this.stage = stage;
            this.joinClause = joinClause;
        }

        ComputeStage<E1> stage() {
            return stage;
        }

        JoinClause<K, E0, E1, E1_OUT> clause() {
            return joinClause;
        }
    }
}
