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

package com.hazelcast.jet.sql.impl.rule;

import com.hazelcast.jet.sql.impl.OptUtils;
import com.hazelcast.jet.sql.impl.rel.FullScanLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

public final class ProjectIntoScanLogicalRule extends RelOptRule {

    public static final ProjectIntoScanLogicalRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
                operand(Project.class, operand(TableScan.class, any())),
                RelFactories.LOGICAL_BUILDER,
                ProjectIntoScanLogicalRule.class.getSimpleName()
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
