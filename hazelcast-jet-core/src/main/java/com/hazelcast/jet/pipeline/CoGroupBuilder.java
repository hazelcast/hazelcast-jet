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
import com.hazelcast.jet.pipeline.datamodel.Tag;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.pipeline.datamodel.Tag.tag;
import static java.util.stream.Collectors.toList;

/**
 * Javadoc pending.
 */
public class CoGroupBuilder<K, E0> {
    private final List<CoGroupClause<?, K>> clauses = new ArrayList<>();

    CoGroupBuilder(ComputeStage<E0> s, DistributedFunction<? super E0, K> groupKeyF) {
        add(s, groupKeyF);
    }

    public Tag<E0> tag0() {
        return Tag.tag0();
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(ComputeStage<E> s, DistributedFunction<? super E, K> groupKeyF) {
        clauses.add(new CoGroupClause<>(s, groupKeyF));
        return (Tag<E>) tag(clauses.size() - 1);
    }

    @SuppressWarnings("unchecked")
    public <A, R> ComputeStage<Tuple2<K, R>> build(AggregateOperation<A, R> aggrOp) {
        List<ComputeStage> upstream = clauses
                .stream()
                .map(CoGroupClause::pstream)
                .collect(toList());
        MultiTransform transform = Transforms.coGroup(clauses
                .stream()
                .map(CoGroupClause::groupKeyF)
                .collect(toList()),
                aggrOp);
        PipelineImpl pipeline = (PipelineImpl) clauses.get(0).pstream.getPipeline();
        return pipeline.attach(upstream, transform);
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
