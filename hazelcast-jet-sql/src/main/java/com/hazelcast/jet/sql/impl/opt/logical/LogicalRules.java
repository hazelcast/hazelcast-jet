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
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class LogicalRules {

    private LogicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                FilterLogicalRule.INSTANCE,
                FilterMergeRule.INSTANCE,

                ProjectLogicalRule.INSTANCE,
                ProjectMergeRule.INSTANCE,
                ProjectRemoveRule.INSTANCE,

                FilterProjectTransposeRule.INSTANCE,
                ProjectFilterTransposeRule.INSTANCE,

                FullScanLogicalRule.INSTANCE,
                FullFunctionScanLogicalRule.INSTANCE,
                ProjectIntoScanLogicalRule.INSTANCE,
                FilterIntoScanLogicalRule.INSTANCE,

                ValuesLogicalRule.INSTANCE,
                InsertLogicalRule.INSTANCE
        );
    }
}
