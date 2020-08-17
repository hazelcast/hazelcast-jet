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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.sql.impl.calcite.opt.cost.Cost;
import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for scans.
 */
public abstract class AbstractFullScanRel extends TableScan {

    private final List<RexNode> projection;

    protected AbstractFullScanRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            List<RexNode> projection
    ) {
        super(cluster, traitSet, table);

        this.projection = projection != null
                ? projection
                : projection(getTable().unwrap(HazelcastTable.class), cluster.getTypeFactory());
    }

    private static List<RexNode> projection(HazelcastTable table, RelDataTypeFactory typeFactory) {
        List<Integer> projects = table.getProjects();

        List<RexNode> projection = new ArrayList<>(projects.size());
        for (Integer index : projects) {
            TableField field = table.getTarget().getField(index);

            RelDataType relDataType = HazelcastSchemaUtils.convert(field, typeFactory);

            projection.add(new RexInputRef(index, relDataType));
        }
        return projection;
    }

    public List<RexNode> getProjection() {
        return projection;
    }

    public RexNode getFilter() {
        return getTable().unwrap(HazelcastTable.class).getFilter();
    }

    /**
     * @return Unwrapped Hazelcast table.
     */
    public Table getTableUnwrapped() {
        return getTable().unwrap(HazelcastTable.class).getTarget();
    }

    @Override
    public final double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = super.estimateRowCount(mq); // TODO not a real row count?
        if (getFilter() != null) {
            double selectivity = mq.getSelectivity(this, getFilter()); // TODO verify this works
            rowCount = rowCount * selectivity;
        }
        return rowCount;
    }

    @Override
    public final RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

        for (int i = 0; i < getProjection().size(); i++) {
            RexNode project = getProjection().get(i);
            builder.add("$" + i, project.getType());
        }

        return builder.build();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                    .item("projection", projection);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        Cost scanCost = (Cost) super.computeSelfCost(planner, mq);

        // 2. Get cost of the project taking in count filter and number of expressions. Project never produces IO.
        double filterRowCount = scanCost.getRowsInternal();

        RexNode filter = getFilter();
        if (filter != null) {
            double filterSelectivity = mq.getSelectivity(this, filter);
            filterRowCount = filterRowCount * filterSelectivity;
        }

        int expressionCount = getProjection().size();
        double projectCpu = CostUtils.adjustCpuForConstrainedScan(filterRowCount * expressionCount);

        // 3. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
                filterRowCount,
                scanCost.getCpuInternal() + projectCpu,
                scanCost.getNetworkInternal()
        );
    }
}
