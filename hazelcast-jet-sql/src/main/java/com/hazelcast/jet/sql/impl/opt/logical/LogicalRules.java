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

package com.hazelcast.jet.sql.impl.opt.logical;

import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class LogicalRules {

    private LogicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // Filter rules.
                FilterLogicalRule.INSTANCE,
                FilterMergeRule.INSTANCE,
                FilterProjectTransposeRule.INSTANCE,
                FilterIntoScanLogicalRule.INSTANCE,

                // Project rules
                ProjectLogicalRule.INSTANCE,
                ProjectMergeRule.INSTANCE,
                ProjectRemoveRule.INSTANCE,
                ProjectFilterTransposeRule.INSTANCE,
                ProjectJoinTransposeRule.INSTANCE,
                ProjectIntoScanLogicalRule.INSTANCE,

                // Scan rules
                FullScanLogicalRule.INSTANCE,
                FullFunctionScanLogicalRule.INSTANCE,

                // Aggregate rules
                AggregateLogicalRule.INSTANCE,

                // Value rules
                ValuesLogicalRule.INSTANCE,
                ValuesReduceRule.FILTER_INSTANCE,
                ValuesReduceRule.PROJECT_INSTANCE,
                ValuesReduceRule.PROJECT_FILTER_INSTANCE,
                ValuesUnionLogicalRule.INSTANCE,

                PruneEmptyRules.PROJECT_INSTANCE,
                PruneEmptyRules.FILTER_INSTANCE,

                // Insert rules
                InsertLogicalRule.INSTANCE
        );
    }
}
