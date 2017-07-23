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

package com.hazelcast.jet.pipeline.impl.transform;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.bag.Tag;

import java.util.List;

/**
 * Javadoc pending.
 */
public class CoGroupTransform<K, A, R> implements JoinTransform {
    private final List<DistributedFunction<?, ? extends K>> groupKeyFns;
    private final AggregateOperation<A, R> aggregateOperation;
    private final List<Tag> tags;

    public CoGroupTransform(
            List<DistributedFunction<?, ? extends K>> groupKeyFns,
            AggregateOperation<A, R> aggregateOperation,
            List<Tag> tags
    ) {
        this.groupKeyFns = groupKeyFns;
        this.aggregateOperation = aggregateOperation;
        this.tags = tags;
    }

    public List<DistributedFunction<?, ? extends K>> groupKeyFns() {
        return groupKeyFns;
    }

    public AggregateOperation<A, R> aggregateOperation() {
        return aggregateOperation;
    }

    public List<Tag> tags() {
        return tags;
    }
}
