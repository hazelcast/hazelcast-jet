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

package com.hazelcast.jet.sql.impl.rel;

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Physical full scan over a source connector.
 */
public class FullScanPhysicalRel extends AbstractFullScanRel implements PhysicalRel {

    public FullScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            List<RexNode> projection,
            RexNode filter
    ) {
        super(cluster, traitSet, table, projection, filter);
    }

    public Expression<Boolean> filter() {
        PlanNodeSchema schema = new PlanNodeSchema(Util.toList(getTableUnwrapped().getFields(), TableField::getType));
        return filter(schema, getFilter());
    }

    public List<Expression<?>> projection() {
        PlanNodeSchema schema = new PlanNodeSchema(Util.toList(getTableUnwrapped().getFields(), TableField::getType));
        return project(schema, getProjection());
    }

    @Override
    public PlanNodeSchema schema() {
        List<QueryDataType> fieldTypes = projection().stream()
                                                     .map(Expression::getType)
                                                     .collect(toList());
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public void visit(CreateDagVisitor visitor) {
        visitor.onConnectorFullScan(this);
    }

    // TODO: Dedup with logical scan
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        RelOptCost scanCost = super.computeSelfCost(planner, mq);

        // 2. Get cost of the project taking in count filter and number of expressions. Project never produces IO.
        double filterRowCount = scanCost.getRows();

        RexNode filter = getFilter();
        if (filter != null) {
            double filterSelectivity = mq.getSelectivity(this, filter);
            filterRowCount = filterRowCount * filterSelectivity;
        }

        int expressionCount = getProjection().size();
        double projectCpu = CostUtils.adjustProjectCpu(filterRowCount * expressionCount, true);

        // 3. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
                filterRowCount,
                scanCost.getCpu() + projectCpu,
                scanCost.getIo()
        );
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FullScanPhysicalRel(getCluster(), traitSet, getTable(), getProjection(), getFilter());
    }
}
