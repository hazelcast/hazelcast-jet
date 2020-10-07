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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class AggregateGroupPhysicalRel extends SingleRel implements PhysicalRel {

    private final AggregateOperation<?, Object[]> aggregateOperation;

    AggregateGroupPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            AggregateOperation<?, Object[]> aggregateOperation

    ) {
        super(cluster, traits, input);

        this.aggregateOperation = aggregateOperation;
    }

    public AggregateOperation<?, Object[]> aggregateOperation() {
        return aggregateOperation;
    }

    @Override
    public PlanNodeSchema schema() {
        // intermediate operator, schema should not be ever needed
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex visit(CreateDagVisitor visitor) {
        return visitor.onGroup(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new AggregateGroupPhysicalRel(getCluster(), traitSet, sole(inputs), aggregateOperation);
    }
}
