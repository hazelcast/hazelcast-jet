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

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpression;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

/**
 * Static utility classes for rules.
 */
public final class OptUtils {

    private OptUtils() {
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
        return RelOptRule.convert(rel, toLogicalConvention(rel.getTraitSet()));
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
        return RelOptRule.convert(rel, toPhysicalConvention(rel.getTraitSet()));
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

    public static HazelcastTable extractHazelcastTable(RelNode rel) {
        HazelcastTable table = rel.getTable().unwrap(HazelcastTable.class);

        assert table != null;

        return table;
    }

    public static List<QueryDataType> extractFieldTypes(RelOptTable relTable) {
        Table table = relTable.unwrap(HazelcastTable.class).getTarget();

        List<QueryDataType> fieldTypes = new ArrayList<>();
        for (TableField field : table.getFields()) {
            // support for discovered maps inserts
            if (!field.isHidden()) {
                fieldTypes.add(field.getType());
            }
        }
        return fieldTypes;
    }

    public static List<QueryDataType> extractFieldTypes(RelDataType rowType) {
        return toList(
                rowType.getFieldList(),
                field -> SqlToQueryType.map(field.getType().getSqlTypeName())
        );
    }

    public static Collection<RelNode> extractPhysicalRelsFromSubset(RelNode node) {
        if (node instanceof RelSubset) {
            RelSubset subset = (RelSubset) node;

            Set<RelTraitSet> traitSets = new HashSet<>();
            Set<RelNode> result = Collections.newSetFromMap(new IdentityHashMap<>());
            for (RelNode rel : subset.getRelList()) {
                if (!isPhysical(rel)) {
                    continue;
                }

                if (traitSets.add(rel.getTraitSet())) {
                    result.add(RelOptRule.convert(node, rel.getTraitSet()));
                }
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    private static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(JetConventions.PHYSICAL);
    }

    public static Collection<RelNode> extractRelsFromSubset(RelNode node) {
        if (node instanceof RelSubset) {
            RelSubset subset = (RelSubset) node;

            Set<RelNode> result = Collections.newSetFromMap(new IdentityHashMap<>());
            for (RelNode rel : subset.getRels()) {
                result.add(rel);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public static LogicalTableScan createLogicalScan(
            TableScan originalScan,
            HazelcastTable newHazelcastTable
    ) {
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) originalScan.getTable();
        HazelcastRelOptTable newTable = createRelTable(
                originalRelTable,
                newHazelcastTable,
                originalScan.getCluster().getTypeFactory()
        );
        return LogicalTableScan.create(originalScan.getCluster(), newTable, originalScan.getHints());
    }

    private static HazelcastRelOptTable createRelTable(
            HazelcastRelOptTable originalRelTable,
            HazelcastTable newHazelcastTable,
            RelDataTypeFactory typeFactory
    ) {
        RelOptTableImpl newTable = RelOptTableImpl.create(
                originalRelTable.getRelOptSchema(),
                newHazelcastTable.getRowType(typeFactory),
                originalRelTable.getDelegate().getQualifiedName(),
                newHazelcastTable,
                null
        );
        return new HazelcastRelOptTable(newTable);
    }

    public static List<Object[]> convert(Values values) {
        List<Object[]> rows = new ArrayList<>(values.getTuples().size());
        for (List<RexLiteral> tuple : values.getTuples()) {

            Object[] result = new Object[tuple.size()];
            for (int i = 0; i < tuple.size(); i++) {
                RexLiteral literal = tuple.get(i);
                Expression<?> expression = RexToExpression.convertLiteral(literal);
                Object value = expression.eval(null, null);
                result[i] = value;
            }
            rows.add(result);
        }
        return rows;
    }

    public static List<Object[]> convert(Values values, RelDataType rowType) {
        List<QueryDataType> types = extractFieldTypes(rowType);

        List<Object[]> rows = new ArrayList<>(values.getTuples().size());
        for (List<RexLiteral> tuple : values.getTuples()) {

            Object[] result = new Object[tuple.size()];
            for (int i = 0; i < tuple.size(); i++) {
                RexLiteral literal = tuple.get(i);
                Expression<?> expression = RexToExpression.convertLiteral(literal);
                Object value = expression.eval(null, null);
                result[i] = types.get(i).convert(value);
            }
            rows.add(result);
        }
        return rows;
    }
}
