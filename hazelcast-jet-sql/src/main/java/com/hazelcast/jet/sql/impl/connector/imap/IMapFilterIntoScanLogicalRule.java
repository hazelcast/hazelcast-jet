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

package com.hazelcast.jet.sql.impl.connector.imap;

import com.hazelcast.jet.sql.impl.OptUtils;
import com.hazelcast.jet.sql.impl.connector.FullScanLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

public final class IMapFilterIntoScanLogicalRule extends RelOptRule {

    public static final IMapFilterIntoScanLogicalRule INSTANCE = new IMapFilterIntoScanLogicalRule();

    private IMapFilterIntoScanLogicalRule() {
        super(
                operand(Filter.class,
                        operandJ(TableScan.class, null, scan -> scan.getTable().unwrap(IMapTable.class) != null, none())),
                RelFactories.LOGICAL_BUILDER,
                IMapFilterIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        TableScan scan = call.rel(1);

        int scanFieldCount = scan.getTable().getRowType().getFieldCount();

        List<RexNode> projectNodes;
        RexNode oldFilter;
        Mapping mapping;
        if (scan instanceof FullScanLogicalRel) {
            FullScanLogicalRel scan0 = (FullScanLogicalRel) scan;
            projectNodes = scan0.getProjectNodes();
            oldFilter = scan0.getFilter();
            // TODO the scan.identity() is wrong
            mapping = Mappings.source(scan.identity(), scanFieldCount);
        } else {
            projectNodes = null;
            oldFilter = null;
            mapping = Mappings.source(scan.identity(), scanFieldCount);
        }

        //Mapping mapping = Mappings.target(scan.identity(), scan.getTable().getRowType().getFieldCount()); // TODO: Old mode
        RexNode newFilter = RexUtil.apply(mapping, filter.getCondition());
        if (oldFilter != null) {
            List<RexNode> nodes = new ArrayList<>(2);
            nodes.add(oldFilter);
            nodes.add(newFilter);

            newFilter = RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), nodes, true);
        }

        FullScanLogicalRel newScan = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                scan.getTable(),
                projectNodes,
                newFilter
        );

        call.transformTo(newScan);
    }
}
