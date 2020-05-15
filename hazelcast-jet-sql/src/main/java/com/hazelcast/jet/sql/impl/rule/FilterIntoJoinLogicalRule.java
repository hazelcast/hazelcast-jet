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
import com.hazelcast.jet.sql.impl.rel.JoinLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;

import static java.util.Arrays.asList;
import static org.apache.calcite.rex.RexUtil.composeConjunction;

public final class FilterIntoJoinLogicalRule extends RelOptRule {

    public static final FilterIntoJoinLogicalRule INSTANCE = new FilterIntoJoinLogicalRule();

    private FilterIntoJoinLogicalRule() {
        super(
                operand(Filter.class, operand(Join.class, any())),
                RelFactories.LOGICAL_BUILDER,
                FilterIntoJoinLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);

        RexNode newCondition = composeConjunction(
                join.getCluster().getRexBuilder(),
                asList(filter.getCondition(), join.getCondition()),
                true
        );

        JoinLogicalRel newJoin = new JoinLogicalRel(
                join.getCluster(),
                OptUtils.toLogicalConvention(join.getTraitSet()),
                join.getLeft(),
                join.getRight(),
                newCondition,
                join.getJoinType()
        );

        call.transformTo(newJoin);
    }
}
