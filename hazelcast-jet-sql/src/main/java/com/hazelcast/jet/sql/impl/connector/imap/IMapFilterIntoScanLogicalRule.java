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

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

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

        List<RexNode> projectNodes;
        RexNode oldFilter;
        if (scan instanceof FullScanLogicalRel) {
            FullScanLogicalRel scan0 = (FullScanLogicalRel) scan;
            projectNodes = scan0.getProjectNodes();
            oldFilter = scan0.getFilter();
        } else {
            projectNodes = null;
            oldFilter = null;
        }

        //Mapping mapping = Mappings.target(scan.identity(), scan.getTable().getRowType().getFieldCount()); // TODO: Old mode
        RexNode newFilter = projectNodes == null ? filter.getCondition()
                : RexUtil.apply(new SubstituteInputRefVisitor(projectNodes), new RexNode[]{filter.getCondition()})[0];
        if (oldFilter != null) {
            newFilter = RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), asList(oldFilter, newFilter), true);
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

    private static class SubstituteInputRefVisitor implements RexVisitor<RexNode> {

        private final List<RexNode> projectNodes;

        protected SubstituteInputRefVisitor(List<RexNode> projectNodes) {
            this.projectNodes = projectNodes;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return projectNodes.get(inputRef.getIndex());
        }

        public RexNode visitLocalRef(RexLocalRef localRef) {
            return localRef;
        }

        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }

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

        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable;
        }

        public RexNode visitCall(RexCall call) {
            List<RexNode> newOperands = new ArrayList<>(call.getOperands().size());
            for (RexNode operand : call.operands) {
                newOperands.add(operand.accept(this));
            }
            return call.clone(call.type, newOperands);
        }

        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        public RexNode visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef;
        }

        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexNode expr = fieldAccess.getReferenceExpr();
            RexNode newOperand = expr.accept(this);
            if (newOperand != fieldAccess.getReferenceExpr()) {
                throw new RuntimeException("replacing partition key not supported");
            }
            return fieldAccess;
        }

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
