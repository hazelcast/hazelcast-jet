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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static com.hazelcast.jet.sql.impl.OptUtils.CONVENTION_PHYSICAL;

public class JetTableInsertPhysicalRel extends TableModify implements PhysicalRel {

    public JetTableInsertPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened
    ) {
        super(cluster, traitSet, table, catalogReader, input, operation,
                updateColumnList, sourceExpressionList, flattened);
        assert input.getConvention() == CONVENTION_PHYSICAL;
        assert getConvention() == CONVENTION_PHYSICAL;
        assert table.unwrap(JetTable.class) != null; // TODO: user error in validator
    }

    @Override
    public PlanNodeSchema schema() {
        return new PlanNodeSchema(table.unwrap(JetTable.class).getFieldTypes());
    }

    @Override
    public void visit(CreateDagVisitor visitor) {
        visitor.onTableInsert(this);

        ((PhysicalRel) getInput()).visit(visitor);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new JetTableInsertPhysicalRel(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                sole(inputs),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened()
        );
    }
}
