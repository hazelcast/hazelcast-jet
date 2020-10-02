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

import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AggregateLogicalRel extends Aggregate implements LogicalRel {

    public AggregateLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        super(cluster, traits, new ArrayList<>(), child, groupSet, groupSets, aggCalls);
    }

    @Override
    public final Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggregateCalls
    ) {
        return new AggregateLogicalRel(getCluster(), traitSet, input, groupSet, groupSets, aggregateCalls);
    }

    /*@Override
    protected RelDataType deriveRowType() {
        return deriveRowType(getCluster().getTypeFactory(), getInput().getRowType(),
                false, groupSet, groupSets, aggCalls);
    }*/

    public static RelDataType deriveRowType(RelDataTypeFactory typeFactory,
                                            final RelDataType inputRowType, boolean indicator,
                                            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                                            final List<AggregateCall> aggCalls) {
        final List<Integer> groupList = groupSet.asList();
        assert groupList.size() == groupSet.cardinality();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        final List<RelDataTypeField> fieldList = inputRowType.getFieldList();
        final Set<String> containedNames = new HashSet<>();
        for (int groupKey : groupList) {
            final RelDataTypeField field = fieldList.get(groupKey);
            containedNames.add(field.getName());
            builder.add(field);
            if (groupSets != null && !allContain(groupSets, groupKey)) {
                builder.nullable(true);
            }
        }
        checkIndicator(indicator);
        for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
            final String base;
            if (aggCall.e.name != null) {
                base = aggCall.e.name;
            } else {
                base = "$f" + (groupList.size() + aggCall.i);
            }
            String name = base;
            int i = 0;
            while (containedNames.contains(name)) {
                name = base + "_" + i++;
            }
            containedNames.add(name);


            if (aggCall.getValue().getAggregation().getKind() == SqlKind.SUM) {
                switch (aggCall.e.getType().getSqlTypeName()) {
                    case TINYINT:
                    case SMALLINT:
                        builder.add(name, typeFactory.createSqlType(SqlTypeName.INTEGER));
                        break;
                    case INTEGER:
                    case BIGINT:
                        builder.add(name, typeFactory.createSqlType(SqlTypeName.BIGINT));
                        break;
                    case DECIMAL:
                        builder.add(name, typeFactory.createSqlType(SqlTypeName.DECIMAL));
                        break;
                    case REAL:
                    case DOUBLE:
                        builder.add(name, typeFactory.createSqlType(SqlTypeName.DOUBLE));
                        break;
                    default:
                        throw QueryException.error("Unsupported operand type: " + aggCall.e.getType());
                }


            } else {
                builder.add(name, aggCall.e.type);
            }
        }
        return builder.build();
    }

    private static boolean allContain(List<ImmutableBitSet> groupSets,
                                      int groupKey) {
        for (ImmutableBitSet groupSet : groupSets) {
            if (!groupSet.get(groupKey)) {
                return false;
            }
        }
        return true;
    }
}
