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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.sql.impl.aggregate.AvgSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.CountSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MaxSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MinSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.SumSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.ValueSqlAggregation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

final class AggregatePhysicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new AggregatePhysicalRule();

    private AggregatePhysicalRule() {
        super(
                operand(AggregateLogicalRel.class, LOGICAL, some(operand(RelNode.class, any()))),
                AggregatePhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel logicalAggregate = call.rel(0);
        RelNode input = logicalAggregate.getInput();

        assert logicalAggregate.getGroupType() == Group.SIMPLE;

        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.extractPhysicalRelsFromSubset(convertedInput);
        for (RelNode transformedInput : transformedInputs) {
            call.transformTo(optimize(logicalAggregate, transformedInput));
        }
    }

    private static RelNode optimize(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        return logicalAggregate.getGroupSet().cardinality() == 0
                ? toAggregate(logicalAggregate, physicalInput)
                : toAggregateByKey(logicalAggregate, physicalInput);
    }

    private static RelNode toAggregate(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        AggregateOperation<SqlAggregations, Object[]> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new AggregatePhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        } else {
            RelNode rel = new AggregateAccumulatePhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    aggrOp
            );

            return new AggregateCombinePhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        }
    }

    private static RelNode toAggregateByKey(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        AggregateOperation<SqlAggregations, Object[]> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new AggregateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        } else {
            RelNode rel = new AggregateAccumulateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    aggrOp
            );

            return new AggregateCombineByKeyPhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        }
    }

    private static AggregateOperation<SqlAggregations, Object[]> aggregateOperation(
            RelDataType inputType,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggregateCalls
    ) {
        List<QueryDataType> operandTypes = OptUtils.schema(inputType).getTypes();

        List<SupplierEx<SqlAggregation>> aggregationProviders = new ArrayList<>();
        for (Integer groupIndex : groupSet.toList()) {
            QueryDataType operandType = operandTypes.get(groupIndex);
            aggregationProviders.add(() -> new ValueSqlAggregation(groupIndex, operandType));
        }
        for (AggregateCall aggregateCall : aggregateCalls) {
            boolean distinct = aggregateCall.isDistinct();
            List<Integer> aggregateCallArguments = aggregateCall.getArgList();
            SqlKind kind = aggregateCall.getAggregation().getKind();
            switch (kind) {
                case COUNT:
                    if (distinct) {
                        int countIndex = aggregateCallArguments.get(0);
                        aggregationProviders.add(() -> new CountSqlAggregation(countIndex, true));
                    } else if (aggregateCallArguments.size() == 1) {
                        int countIndex = aggregateCallArguments.get(0);
                        aggregationProviders.add(() -> new CountSqlAggregation(countIndex));
                    } else {
                        aggregationProviders.add(CountSqlAggregation::new);
                    }
                    break;
                case MIN:
                    int minIndex = aggregateCallArguments.get(0);
                    QueryDataType minOperandType = operandTypes.get(minIndex);
                    aggregationProviders.add(() -> new MinSqlAggregation(minIndex, minOperandType));
                    break;
                case MAX:
                    int maxIndex = aggregateCallArguments.get(0);
                    QueryDataType maxOperandType = operandTypes.get(maxIndex);
                    aggregationProviders.add(() -> new MaxSqlAggregation(maxIndex, maxOperandType));
                    break;
                case SUM:
                    int sumIndex = aggregateCallArguments.get(0);
                    QueryDataType sumOperandType = operandTypes.get(sumIndex);
                    aggregationProviders.add(() -> new SumSqlAggregation(sumIndex, sumOperandType, distinct));
                    break;
                case AVG:
                    int avgIndex = aggregateCallArguments.get(0);
                    QueryDataType avgOperandType = operandTypes.get(avgIndex);
                    aggregationProviders.add(() -> new AvgSqlAggregation(avgIndex, avgOperandType, distinct));
                    break;
                default:
                    throw QueryException.error("Unsupported aggregation function: " + kind);
            }
        }

        return AggregateOperation
                .withCreate(() -> {
                    SqlAggregation[] aggregations = new SqlAggregation[aggregationProviders.size()];
                    for (int i = 0; i < aggregationProviders.size(); i++) {
                        aggregations[i] = aggregationProviders.get(i).get();
                    }
                    return new SqlAggregations(aggregations);
                })
                .andAccumulate(SqlAggregations::accumulate)
                .andCombine(SqlAggregations::combine)
                .andExportFinish(SqlAggregations::collect);
    }
}
