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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;
import static org.apache.calcite.plan.RelOptRule.convert;

/**
 * Static utility classes for rules.
 */
public final class OptUtils {

    private OptUtils() {
    }

    /**
     * Get operand matching a single node.
     *
     * @param cls        Node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R extends RelNode> RelOptRuleOperand single(Class<R> cls, Convention convention) {
        return RelOptRule.operand(cls, convention, RelOptRule.any());
    }

    /**
     * Get operand matching a node with specific child node.
     *
     * @param cls        Node class.
     * @param childCls   Child node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode> RelOptRuleOperand parentChild(
            Class<R1> cls,
            Class<R2> childCls, Convention convention
    ) {
        RelOptRuleOperand childOperand = RelOptRule.operand(childCls, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand));
    }

    /**
     * Get operand matching a node with specific pair of child nodes.
     *
     * @param cls        Node class.
     * @param childCls1  Child node class 1.
     * @param childCls2  Child node class 2.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode, R3 extends RelNode> RelOptRuleOperand parentChildChild(
            Class<R1> cls,
            Class<R2> childCls1,
            Class<R3> childCls2,
            Convention convention
    ) {
        RelOptRuleOperand childOperand1 = RelOptRule.operand(childCls1, RelOptRule.any());
        RelOptRuleOperand childOperand2 = RelOptRule.operand(childCls2, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand1, childOperand2));
    }

    /**
     * Add a single trait to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait    Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait) {
        return traitSet.plus(trait).simplify();
    }

    /**
     * Convert the given trait set to logical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with logical convention.
     */
    public static RelTraitSet toLogicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, LOGICAL);
    }

    /**
     * Convert the given input into logical input.
     *
     * @param rel Original input.
     * @return Logical input.
     */
    public static RelNode toLogicalInput(RelNode rel) {
        return convert(rel, toLogicalConvention(rel.getTraitSet()));
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, PHYSICAL);
    }

    /**
     * Convert the given input into physical input.
     *
     * @param rel Original input.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode rel) {
        return convert(rel, toPhysicalConvention(rel.getTraitSet()));
    }

    public static List<QueryDataType> getFieldTypes(RelOptTable relTable) {
        Table table = relTable.unwrap(HazelcastTable.class).getTarget();

        List<QueryDataType> fieldTypes = new ArrayList<>();
        for (TableField field : table.getFields()) {
            if (!field.isHidden()) {
                fieldTypes.add(field.getType());
            }
        }
        return fieldTypes;
    }

    public static HazelcastTable getHazelcastTable(RelNode rel) {
        HazelcastTable table = rel.getTable().unwrap(HazelcastTable.class);

        assert table != null;

        return table;
    }

    public static HazelcastRelOptTable createRelTable(
            HazelcastRelOptTable originalRelTable,
            HazelcastTable convertedHazelcastTable,
            RelDataTypeFactory typeFactory
    ) {
        RelOptTableImpl newTable = RelOptTableImpl.create(
                originalRelTable.getRelOptSchema(),
                convertedHazelcastTable.getRowType(typeFactory),
                originalRelTable.getDelegate().getQualifiedName(),
                convertedHazelcastTable,
                null
        );

        return new HazelcastRelOptTable(newTable);
    }

    public static Collection<RelNode> getPhysicalRelsFromSubset(RelNode subset) {
        if (subset instanceof RelSubset) {
            RelSubset subset0 = (RelSubset) subset;

            Set<RelTraitSet> traitSets = new HashSet<>();

            Set<RelNode> res = Collections.newSetFromMap(new IdentityHashMap<>());

            for (RelNode rel : subset0.getRelList()) {
                if (!isPhysical(rel)) {
                    continue;
                }

                if (traitSets.add(rel.getTraitSet())) {
                    res.add(convert(subset, rel.getTraitSet()));
                }
            }

            return res;
        } else {
            return Collections.emptyList();
        }
    }

    private static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(JetConventions.PHYSICAL);
    }
}
