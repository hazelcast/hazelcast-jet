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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public final class Aggregators {

    private Aggregators() {
    }

    public static FunctionEx<?, ?> keyFn(
            ImmutableBitSet groupSet
    ) {
        List<Integer> groupIndices = groupSet.toList();

        return (FunctionEx<Object[], Object>) row -> {
            Object[] key = new Object[groupIndices.size()];
            for (int i = 0; i < groupIndices.size(); i++) {
                key[i] = row[groupIndices.get(i)];
            }
            return new ObjectArray(key);
        };
    }

    public static AggregateOperation<Aggregator, Object[]> operation(
            ImmutableBitSet groupSet,
            List<AggregateCall> aggregateCalls,
            PlanNodeSchema inputSchema
    ) {
        // TODO: DISTINCT ???

        List<Integer> groupIndices = groupSet.toList();

        List<SqlKind> aggregateFunctionKinds = new ArrayList<>();
        List<List<Integer>> aggregateFunctionArgumentIndices = new ArrayList<>();
        for (AggregateCall aggregateCall : aggregateCalls) {
            aggregateFunctionKinds.add(aggregateCall.getAggregation().getKind());
            aggregateFunctionArgumentIndices.add(aggregateCall.getArgList());
        }
        List<QueryDataType> inputTypes = inputSchema.getTypes();

        return AggregateOperation
                .withCreate(() -> aggregator(
                        groupIndices,
                        aggregateFunctionKinds,
                        aggregateFunctionArgumentIndices,
                        inputTypes
                ))
                .andAccumulate(Aggregator::accumulate)
                .andCombine(Aggregator::combine)
                // TODO: deduct ???
                .andExportFinish(Aggregator::collect);
    }

    private static Aggregator aggregator(
            List<Integer> groupIndices,
            List<SqlKind> aggregateFunctionKinds,
            List<List<Integer>> aggregateFunctionArgumentIndices,
            List<QueryDataType> operandTypes
    ) {
        List<Aggregation> aggregators = new ArrayList<>(groupIndices.size() + aggregateFunctionKinds.size());

        for (Integer groupIndex : groupIndices) {
            aggregators.add(new ValueAggregation(groupIndex, operandTypes.get(groupIndex)));
        }

        for (int i = 0; i < aggregateFunctionKinds.size(); i++) {
            SqlKind kind = aggregateFunctionKinds.get(i);
            List<Integer> argumentIndices = aggregateFunctionArgumentIndices.get(i);
            switch (kind) {
                // TODO: check IMDG mapping
                case COUNT:
                    aggregators.add(new CountAggregation());
                    break;
                case MIN:
                    aggregators.add(new MinAggregation(argumentIndices.get(0), operandTypes.get(argumentIndices.get(0))));
                    break;
                case MAX:
                    aggregators.add(new MaxAggregation(argumentIndices.get(0), operandTypes.get(argumentIndices.get(0))));
                    break;
                case SUM:
                    aggregators.add(new SumAggregation(argumentIndices.get(0), operandTypes.get(argumentIndices.get(0))));
                    break;
                case AVG:
                    aggregators.add(new AvgAggregation(argumentIndices.get(0), operandTypes.get(argumentIndices.get(0))));
                    break;
                default:
                    throw QueryException.error("Unsupported aggregation: " + kind);
            }
        }

        return new Aggregator(aggregators.toArray(new Aggregation[0]));
    }
}
