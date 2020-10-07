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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.aggregate.Aggregations;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class AggregateCombinePhysicalRel extends Aggregate implements PhysicalRel {

    private final FunctionEx<Object, Object> partitionKeyFn;
    private final AggregateOperation<Aggregations, Object[]> aggregateOperation;

    AggregateCombinePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            FunctionEx<Object, Object> partitionKeyFn,
            AggregateOperation<Aggregations, Object[]> aggregateOperation
    ) {
        super(cluster, traits, new ArrayList<>(), input, groupSet, groupSets, aggCalls);

        this.partitionKeyFn = partitionKeyFn;
        this.aggregateOperation = aggregateOperation;
    }

    public FunctionEx<Object, Object> partitionKeyFn() {
        return partitionKeyFn;
    }

    public AggregateOperation<Aggregations, Object[]> aggregateOperation() {
        return aggregateOperation;
    }

    @Override
    public PlanNodeSchema schema() {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex visit(CreateDagVisitor visitor) {
        return visitor.onCombine(this);
    }

    @Override
    public final Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        return new AggregateCombinePhysicalRel(
                getCluster(),
                traitSet,
                input,
                groupSet,
                groupSets,
                aggCalls,
                partitionKeyFn,
                aggregateOperation
        );
    }
}
