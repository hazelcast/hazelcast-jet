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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

final class FilterIntoScanLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new FilterIntoScanLogicalRule();

    private FilterIntoScanLogicalRule() {
        super(
                operand(Filter.class, operand(TableScan.class, any())), RelFactories.LOGICAL_BUILDER,
                FilterIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        TableScan scan = call.rel(1);

        HazelcastTable table = OptUtils.getHazelcastTable(scan);

        RexNode filterCondition = filter.getCondition();

        List<RexNode> projection = scan instanceof FullScanLogicalRel
                ? ((FullScanLogicalRel) scan).getProjection()
                : null;

        RexNode convertedCondition = projection == null
                ? RexUtil.apply(Mappings.source(table.getProjects(), table.getOriginalFieldCount()), filterCondition)
                : RexUtil.apply(new ProjectFieldVisitor(projection), new RexNode[]{filterCondition})[0];

        RexNode tableFilter = table.getFilter();
        if (tableFilter != null) {
            convertedCondition = RexUtil.composeConjunction(
                    scan.getCluster().getRexBuilder(),
                    asList(tableFilter, convertedCondition),
                    true
            );
        }

        RelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                table.withFilter(convertedCondition),
                scan.getCluster().getTypeFactory()
        );

        FullScanLogicalRel rel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                projection
        );
        call.transformTo(rel);
    }

    private static class ProjectFieldVisitor implements RexVisitor<RexNode> {

        private final List<RexNode> projection;

        protected ProjectFieldVisitor(List<RexNode> projection) {
            this.projection = projection;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return projection.get(inputRef.getIndex());
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef localRef) {
            return localRef;
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }

        @Override
        public RexNode visitOver(RexOver over) {
            final RexWindow window = over.getWindow();
            for (RexFieldCollation orderKey : window.orderKeys) {
                RexNode newOrderKey = orderKey.left.accept(this);
                if (newOrderKey != orderKey.left) {
                    throw new RuntimeException("replacing order key not supported");
                }
            }
            for (RexNode partitionKey : window.partitionKeys) {
                RexNode newPartitionKey = partitionKey.accept(this);
                if (partitionKey != newPartitionKey) {
                    throw new RuntimeException("replacing partition key not supported");
                }
            }
            window.getLowerBound().accept(this);
            window.getUpperBound().accept(this);
            return over;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            List<RexNode> newOperands = new ArrayList<>(call.getOperands().size());
            for (RexNode operand : call.operands) {
                newOperands.add(operand.accept(this));
            }
            return call.clone(call.type, newOperands);
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        @Override
        public RexNode visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexNode expr = fieldAccess.getReferenceExpr();
            RexNode newOperand = expr.accept(this);
            if (newOperand != fieldAccess.getReferenceExpr()) {
                throw new RuntimeException("replacing partition key not supported");
            }
            return fieldAccess;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            List<RexNode> newOperands = new ArrayList<>(subQuery.operands.size());
            for (RexNode operand : subQuery.operands) {
                newOperands.add(operand.accept(this));
            }
            return subQuery.clone(subQuery.type, newOperands);
        }

        @Override
        public RexNode visitTableInputRef(RexTableInputRef ref) {
            return ref;
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return fieldRef;
        }
    }
}
