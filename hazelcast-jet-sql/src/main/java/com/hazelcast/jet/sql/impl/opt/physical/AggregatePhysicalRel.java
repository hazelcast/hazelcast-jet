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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.aggregate.Aggregation;
import com.hazelcast.jet.sql.impl.aggregate.Aggregations;
import com.hazelcast.jet.sql.impl.aggregate.AvgAggregation;
import com.hazelcast.jet.sql.impl.aggregate.CountAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MaxAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MinAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SumAggregation;
import com.hazelcast.jet.sql.impl.aggregate.ValueAggregation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregatePhysicalRel extends Aggregate implements PhysicalRel {

    private static final String ALL_GROUP_KEY = "";

    AggregatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        super(cluster, traits, new ArrayList<>(), child, groupSet, groupSets, aggCalls);
    }

    public FunctionEx<Object[], Object> groupKeyFn() {
        List<Integer> groupIndices = getGroupSet().toList();

        if (groupIndices.size() == 0) {
            return row -> ALL_GROUP_KEY;
        } else if (groupIndices.size() == 1) {
            return row -> row[groupIndices.get(0)];
        } else {
            return row -> {
                Object[] key = new Object[groupIndices.size()];
                for (int i = 0; i < groupIndices.size(); i++) {
                    key[i] = row[groupIndices.get(i)];
                }
                return new ObjectArray(key);
            };
        }
    }

    public AggregateOperation<Aggregations, Object[]> aggregateOperation() {
        List<QueryDataType> operandTypes = ((PhysicalRel) getInput()).schema().getTypes();

        List<SupplierEx<Aggregation>> aggregationProviders = new ArrayList<>();
        for (Integer groupIndex : getGroupSet().toList()) {
            QueryDataType operandType = operandTypes.get(groupIndex);
            aggregationProviders.add(() -> new ValueAggregation(groupIndex, operandType));
        }
        for (AggregateCall aggregateCall : getAggCallList()) {
            SqlKind kind = aggregateCall.getAggregation().getKind();
            switch (kind) {
                case COUNT:
                    aggregationProviders.add(CountAggregation::new);
                    break;
                case MIN:
                    int minIndex = aggregateCall.getArgList().get(0);
                    QueryDataType minOperandType = operandTypes.get(minIndex);
                    aggregationProviders.add(() -> new MinAggregation(minIndex, minOperandType));
                    break;
                case MAX:
                    int maxIndex = aggregateCall.getArgList().get(0);
                    QueryDataType maxOperandType = operandTypes.get(maxIndex);
                    aggregationProviders.add(() -> new MaxAggregation(maxIndex, maxOperandType));
                    break;
                case SUM:
                    int sumIndex = aggregateCall.getArgList().get(0);
                    QueryDataType sumOperandType = operandTypes.get(sumIndex);
                    aggregationProviders.add(() -> new SumAggregation(sumIndex, sumOperandType));
                    break;
                case AVG:
                    int avgIndex = aggregateCall.getArgList().get(0);
                    QueryDataType avgOperandType = operandTypes.get(avgIndex);
                    aggregationProviders.add(() -> new AvgAggregation(avgIndex, avgOperandType));
                    break;
                default:
                    throw QueryException.error("Unsupported aggregation: " + kind);
            }
        }

        return AggregateOperation
                .withCreate(() -> {
                    Aggregation[] aggregations = new Aggregation[aggregationProviders.size()];
                    for (int i = 0; i < aggregationProviders.size(); i++) {
                        aggregations[i] = aggregationProviders.get(i).get();
                    }
                    return new Aggregations(aggregations);
                })
                .andAccumulate(Aggregations::accumulate)
                .andCombine(Aggregations::combine)
                .andExportFinish(Aggregations::collect);
    }

    @Override
    public PlanNodeSchema schema() {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex visit(CreateDagVisitor visitor) {
        return visitor.onAggregate(this);
    }

    @Override
    public final Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        return new AggregatePhysicalRel(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    private static class ObjectArray implements DataSerializable {

        private Object[] array;

        @SuppressWarnings("unused")
        private ObjectArray() {
        }

        private ObjectArray(Object[] array) {
            this.array = array;
        }

        @Override
        public String toString() {
            return "ObjectArray{" +
                    "array=" + Arrays.toString(array) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ObjectArray that = (ObjectArray) o;
            return Arrays.equals(array, that.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(array);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            array = in.readObject();
        }
    }
}
