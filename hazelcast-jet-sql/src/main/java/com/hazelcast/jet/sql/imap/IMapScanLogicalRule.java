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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

import static com.hazelcast.jet.sql.OptUtils.CONVENTION_LOGICAL;

public final class IMapScanLogicalRule extends ConverterRule {
    public static final RelOptRule INSTANCE = new IMapScanLogicalRule();

    private IMapScanLogicalRule() {
        super(LogicalTableScan.class, Convention.NONE, CONVENTION_LOGICAL, IMapScanLogicalRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;

        return new IMapScanLogicalRel(
            scan.getCluster(),
            OptUtils.toLogicalConvention(scan.getTraitSet()),
            scan.getTable(),
            null,
            null
        );
    }
}
