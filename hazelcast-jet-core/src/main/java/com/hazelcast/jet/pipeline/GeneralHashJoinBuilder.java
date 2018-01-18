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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

/**
 * Offers a step-by-step fluent API to build a hash-join pipeline pipeline.
 * To obtain it, call {@link GeneralStage#hashJoinBuilder()} on the primary
 * pipeline, whose data will be enriched from all other stages.
 * <p>
 * This object is mainly intended to build a hash-join of the primary pipeline
 * with three or more contributing stages. For one or two stages the direct
 * {@code pipeline.hashJoin(...)} calls should be preferred because they offer
 * more static type safety.
 *
 * @param <T0> the type of the stream-0 item
 */
public abstract class GeneralHashJoinBuilder<T0, OUT_STAGE extends GeneralStage<Tuple2<T0, ItemsByTag>>> {
    private final Transform transform0;
    private final PipelineImpl pipelineImpl;
    private final CreateOutStageFn<T0, OUT_STAGE> createOutStageFn;
    private final Map<Tag<?>, TransformAndClause> clauses = new HashMap<>();

    GeneralHashJoinBuilder(GeneralStage<T0> stage0, CreateOutStageFn<T0, OUT_STAGE> createOutStageFn) {
        this.transform0 = transformOf(stage0);
        this.pipelineImpl = (PipelineImpl) stage0.getPipeline();
        this.createOutStageFn = createOutStageFn;
    }

    /**
     * Adds another contributing pipeline pipeline to the hash-join operation.
     *
     * @param stage the contributing pipeline
     * @param joinClause specifies how to join the contributing pipeline
     * @param <K> the type of the join key
     * @param <T1_IN> the type of the contributing pipeline's data
     * @param <T1> the type of result after applying the projecting transformation
     *             to the contributing pipeline's data
     * @return the tag that refers to the contributing pipeline
     */
    public <K, T1_IN, T1> Tag<T1> add(BatchStage<T1_IN> stage, JoinClause<K, T0, T1_IN, T1> joinClause) {
        Tag<T1> tag = tag(clauses.size());
        clauses.put(tag, new TransformAndClause<>(stage, joinClause));
        return tag;
    }

    /**
     * Builds a new pipeline pipeline that performs the hash-join operation. The
     * pipeline is attached to all the contributing stages.
     *
     * @return the hash-join pipeline pipeline
     */
    @SuppressWarnings("unchecked")
    public OUT_STAGE build() {
        List<Entry<Tag<?>, TransformAndClause>> orderedClauses = clauses.entrySet().stream()
                                                                        .sorted(comparing(Entry::getKey))
                                                                        .collect(toList());
        List<Transform> upstream =
                concat(
                        Stream.of(transform0),
                        orderedClauses.stream().map(e -> e.getValue().transform())
                ).collect(toList());
        // A probable javac bug forced us to extract this variable
        Stream<JoinClause<?, T0, ?, ?>> joinClauses = orderedClauses
                .stream()
                .skip(1)
                .map(e -> e.getValue().clause());
        HashJoinTransform<T0, Tuple2<T0, ItemsByTag>> hashJoinTransform = new HashJoinTransform<>(
                upstream,
                joinClauses.collect(toList()),
                orderedClauses.stream()
                              .map(Entry::getKey)
                              .collect(toList()));
        pipelineImpl.connect(upstream, hashJoinTransform);
        return createOutStageFn.get(hashJoinTransform, pipelineImpl);
    }

    @FunctionalInterface
    public interface CreateOutStageFn<T0, OUT_STAGE> {
        OUT_STAGE get(
                HashJoinTransform<T0, Tuple2<T0, ItemsByTag>> hashJoinTransform,
                PipelineImpl pipeline);
    }

    private static class TransformAndClause<K, E0, T1, T1_OUT> {
        private final Transform transform;
        private final JoinClause<K, E0, T1, T1_OUT> joinClause;

        @SuppressWarnings("unchecked")
        TransformAndClause(GeneralStage<T1> stage, JoinClause<K, E0, T1, T1_OUT> joinClause) {
            this.transform = transformOf(stage);
            this.joinClause = joinClause;
        }

        Transform transform() {
            return transform;
        }

        JoinClause<K, E0, T1, T1_OUT> clause() {
            return joinClause;
        }
    }
}
