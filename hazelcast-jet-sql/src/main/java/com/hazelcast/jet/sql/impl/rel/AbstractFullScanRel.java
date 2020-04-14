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

import com.hazelcast.jet.sql.impl.schema.JetTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Base class for scans.
 */
public abstract class AbstractFullScanRel extends TableScan {

    /**
     * Projection.
     */
    private final List<RexNode> projection;

    /**
     * Filter.
     */
    private final RexNode filter;

    protected AbstractFullScanRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            List<RexNode> projection,
            RexNode filter
    ) {
        super(cluster, traitSet, table);
        this.projection = projection != null ? projection :
                table.getRowType().getFieldList().stream()
                     .map(field -> new RexInputRef(field.getIndex(), field.getType()))
                     .collect(toList());
        this.filter = filter;
    }

    public List<RexNode> getProjection() {
        return projection;
    }

    public RexNode getFilter() {
        return filter;
    }

    /**
     * @return Unwrapped Hazelcast table.
     */
    public JetTable getTableUnwrapped() {
        return table.unwrap(JetTable.class);
    }

    @Override
    public final double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = super.estimateRowCount(mq); // TODO not a real row count?
        if (filter != null) {
            double selectivity = mq.getSelectivity(this, filter); // TODO verify this works
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
                    .item("projects", getProjection())
                    .item("filter", getFilter());
    }
}
