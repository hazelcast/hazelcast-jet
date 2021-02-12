/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.tools.RelBuilderFactory;

// TODO: get rid of abstract...
abstract class ValuesReduceRule extends RelOptRule {

    static final ValuesReduceRule FILTER_INSTANCE =
            new ValuesReduceRule(
                    operand(LogicalFilter.class, operand(LogicalValues.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRule.class.getSimpleName() + "(Filter)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Filter filter = call.rel(0);
                    Values values = call.rel(1);
                    ReducedLogicalValues rel = ReducedLogicalValues.create(
                            values.getCluster(),
                            filter.getRowType(),
                            values.getTuples(),
                            filter.getCondition(),
                            null
                    );
                    call.transformTo(rel);
                }
            };

    static final ValuesReduceRule PROJECT_INSTANCE =
            new ValuesReduceRule(
                    operand(LogicalProject.class, operand(LogicalValues.class, none())),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRule.class.getSimpleName() + "(Project)"
            ) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Values values = call.rel(1);
                    ReducedLogicalValues rel = ReducedLogicalValues.create(
                            values.getCluster(),
                            project.getRowType(),
                            values.getTuples(),
                            null,
                            project.getProjects()
                    );
                    call.transformTo(rel);
                }
            };

    static final ValuesReduceRule PROJECT_FILTER_INSTANCE =
            new ValuesReduceRule(
                    operand(LogicalProject.class, operand(LogicalFilter.class, operand(LogicalValues.class, none()))),
                    RelFactories.LOGICAL_BUILDER,
                    ValuesReduceRule.class.getSimpleName() + "(Project-Filter)"
            ) {
                public void onMatch(RelOptRuleCall call) {
                    Project project = call.rel(0);
                    Filter filter = call.rel(1);
                    Values values = call.rel(2);
                    ReducedLogicalValues rel = ReducedLogicalValues.create(
                            values.getCluster(),
                            project.getRowType(),
                            values.getTuples(),
                            filter.getCondition(),
                            project.getProjects()
                    );
                    call.transformTo(rel);
                }
            };

    private ValuesReduceRule(
            RelOptRuleOperand operand,
            RelBuilderFactory relBuilderFactory,
            String description
    ) {
        super(operand, relBuilderFactory, description);
    }
}
