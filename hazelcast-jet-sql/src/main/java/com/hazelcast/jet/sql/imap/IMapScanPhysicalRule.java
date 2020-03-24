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

package com.hazelcast.jet.sql.imap;

import com.hazelcast.jet.sql.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import static com.hazelcast.jet.sql.OptUtils.CONVENTION_LOGICAL;

/**
 * Convert logical map scan to either replicated or partitioned physical scan.
 */
public final class IMapScanPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new IMapScanPhysicalRule();

    private IMapScanPhysicalRule() {
        super(OptUtils.single(IMapScanLogicalRel.class, CONVENTION_LOGICAL),
            IMapScanPhysicalRule.class.getSimpleName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IMapScanLogicalRel scan = call.rel(0);
        System.out.println("onMatch0 in " + getClass().getSimpleName());

        // Add normal map scan.
        call.transformTo(new IMapScanPhysicalRel(
            scan.getCluster(),
            OptUtils.toPhysicalConvention(scan.getTraitSet()),
            scan.getTable(),
            scan.getProjects(),
            scan.getFilter()
        ));
    }
}
