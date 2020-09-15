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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.AbstractFullScanRel;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.CreateDagVisitor;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;

public class FullScanPhysicalRel extends AbstractFullScanRel implements PhysicalRel {

    FullScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            List<RexNode> projection
    ) {
        super(cluster, traitSet, table, projection);
    }

    public Expression<Boolean> filter() {
        PlanNodeSchema schema = new PlanNodeSchema(OptUtils.getFieldTypes(getTable()));
        return filter(schema, getFilter());
    }

    public List<Expression<?>> projection() {
        PlanNodeSchema schema = new PlanNodeSchema(OptUtils.getFieldTypes(getTable()));
        return project(schema, getProjection());
    }

    @Override
    public PlanNodeSchema schema() {
        List<QueryDataType> fieldTypes = toList(projection(), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex visit(CreateDagVisitor visitor) {
        return visitor.onFullScan(this);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FullScanPhysicalRel(getCluster(), traitSet, getTable(), getProjection());
    }
}
