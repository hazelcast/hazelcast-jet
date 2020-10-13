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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.sql.impl.aggregate.Aggregations;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.singletonList;

public final class AggregationCombiner extends AbstractProcessor {

    private final Map<Object, Aggregations> keyToAggregations;
    private final FunctionEx<Object, Object> partitionKeyFn;
    private final AggregateOperation<Aggregations, Object[]> aggregationOperation;

    private Traverser<Object[]> resultTraverser;

    public AggregationCombiner(
            FunctionEx<Object, Object> partitionKeyFn,
            AggregateOperation<Aggregations, Object[]> aggregationOperation
    ) {
        this.keyToAggregations = new HashMap<>();
        this.partitionKeyFn = partitionKeyFn;
        this.aggregationOperation = aggregationOperation;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Aggregations aggregations = keyToAggregations.computeIfAbsent(
                partitionKeyFn.apply(item),
                key -> aggregationOperation.createFn().get()
        );
        aggregationOperation.accumulateFn(ordinal).accept(aggregations, item);
        return true;
    }

    @Override
    public boolean complete() {
        if (resultTraverser == null) {
            resultTraverser = new ResultTraverser()
                    .map(aggregations -> aggregationOperation.finishFn().apply(aggregations));
        }
        return emitFromTraverser(resultTraverser);
    }

    private final class ResultTraverser implements Traverser<Aggregations> {

        private final Iterator<Aggregations> aggregations;

        private ResultTraverser() {
            this.aggregations = keyToAggregations.isEmpty()
                    ? singletonList(aggregationOperation.createFn().get()).iterator()
                    : keyToAggregations.values().iterator();
        }

        @Override
        public Aggregations next() {
            if (!aggregations.hasNext()) {
                return null;
            }
            try {
                return aggregations.next();
            } finally {
                aggregations.remove();
            }
        }
    }
}
