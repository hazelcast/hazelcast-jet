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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.evaluate;

public interface JetTableFunction extends TableFunction {

    default HazelcastTable toTable(
            RelDataType rowType,
            List<RexNode> operands,
            RelDataTypeFactory typeFactory
    ) {
        List<QueryDataType> argumentTypes = toList(
                getParameters(),
                parameter -> SqlToQueryType.map(parameter.getType(typeFactory).getSqlTypeName())
        );
        PlanNodeSchema argumentSchema = new PlanNodeSchema(argumentTypes);
        RexVisitor<Expression<?>> argumentConverter = OptUtils.converter(argumentSchema);

        List<Object> arguments = new ArrayList<>();
        for (RexNode operand : operands) {
            // TODO: implement DEFAULT handling
            if (operand instanceof RexCall && ((RexCall) operand).getOperator() == SqlStdOperatorTable.DEFAULT) {
                arguments.add(null);
            } else {
                arguments.add(evaluate(operand.accept(argumentConverter), EmptyRow.INSTANCE));
            }
        }

        List<MappingField> fields = toList(
                rowType.getFieldList(),
                field -> new MappingField(field.getName(), SqlToQueryType.map(field.getType().getSqlTypeName()))
        );

        return table(arguments, fields);
    }

    HazelcastTable table(List<Object> arguments, List<MappingField> fields);
}
