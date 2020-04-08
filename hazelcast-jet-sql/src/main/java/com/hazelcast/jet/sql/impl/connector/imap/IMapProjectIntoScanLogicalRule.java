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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

public final class IMapProjectIntoScanLogicalRule extends RelOptRule {

    public static final IMapProjectIntoScanLogicalRule INSTANCE = new IMapProjectIntoScanLogicalRule();

    private IMapProjectIntoScanLogicalRule() {
        super(
                operand(Project.class,
                        operandJ(TableScan.class, null, scan -> scan.getTable().unwrap(IMapTable.class) != null, none())),
                RelFactories.LOGICAL_BUILDER,
                IMapProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        FullScanLogicalRel fullScanRel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                scan.getTable(),
                project.getProjects(),
                getScanFilter(scan)
        );
        call.transformTo(fullScanRel);
    }

    /**
     * Get filter associated with the scan, if any.
     *
     * @param scan Scan.
     * @return Filter or null.
     */
    private static RexNode getScanFilter(TableScan scan) {
        return scan instanceof FullScanLogicalRel ? ((FullScanLogicalRel) scan).getFilter() : null;
    }
}
