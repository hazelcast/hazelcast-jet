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

import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;

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
                    apply(call, null, filter, values);
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
                    apply(call, project, null, values);
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
                    apply(call, project, filter, values);
                }
            };

    private ValuesReduceRule(
            RelOptRuleOperand operand,
            RelBuilderFactory relBuilderFactory,
            String description
    ) {
        super(operand, relBuilderFactory, description);
    }

    @SuppressWarnings("unchecked")
    protected void apply(
            RelOptRuleCall call,
            Project project,
            Filter filter,
            Values values
    ) {
        PlanNodeSchema schema = new PlanNodeSchema(OptUtils.extractFieldTypes(values.getRowType()));
        // TODO: pass actual parameter metadata, see JetSqlCoreBackendImpl#execute
        RexToExpressionVisitor converter = new RexToExpressionVisitor(schema, new QueryParameterMetadata());

        RelDataType rowType = null;

        Expression<Boolean> predicate = null;
        if (filter != null) {
            rowType = filter.getRowType();
            predicate = (Expression<Boolean>) filter.getCondition().accept(converter);
        }
        List<Expression<?>> projection = null;
        if (project != null) {
            rowType = project.getRowType();
            projection = toList(project.getProjects(), node -> node.accept(converter));
        }

        assert rowType != null;

        List<Object[]> rows = ExpressionUtil.evaluate(predicate, projection, OptUtils.reduce(values));

        ValuesLogicalRel rel = new ValuesLogicalRel(
                values.getCluster(),
                values.getTraitSet(),
                rowType,
                rows
        );
        call.transformTo(rel);
    }
}
