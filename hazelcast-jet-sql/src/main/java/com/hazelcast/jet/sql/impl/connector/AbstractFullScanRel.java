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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.jet.sql.impl.connector.imap.IMapTable;
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
    private final List<RexNode> projectNodes;

    /**
     * Filter.
     */
    private final RexNode filter;

    protected AbstractFullScanRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            List<RexNode> projectNodes,
            RexNode filter
    ) {
        super(cluster, traitSet, table);
        this.projectNodes = projectNodes != null ? projectNodes :
                table.getRowType().getFieldList().stream()
                     .map(field -> new RexInputRef(field.getIndex(), field.getType()))
                     .collect(toList());
        this.filter = filter;
    }

    public List<RexNode> getProjectNodes() {
        return projectNodes;
    }

    public RexNode getFilter() {
        return filter;
    }

    /**
     * @return Unwrapped Hazelcast table.
     */
    public IMapTable getTableUnwrapped() {
        return table.unwrap(IMapTable.class);
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

        for (int i = 0; i < getProjectNodes().size(); i++) {
            RexNode project = getProjectNodes().get(i);
            builder.add("$" + i, project.getType());
        }

        return builder.build();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                    .item("projects", getProjectNodes())
                    .item("filter", getFilter());
    }
}
