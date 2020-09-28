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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.impl.connector.file.FileTableFunction;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER;

public final class JetSqlOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SqlFunction CSV_FILE = from(FileTableFunction.CSV, "CSV_FILE");
    public static final SqlFunction JSON_FILE = from(FileTableFunction.JSON, "JSON_FILE");
    public static final SqlFunction AVRO_FILE = from(FileTableFunction.AVRO, "AVRO_FILE");
    public static final SqlFunction PARQUET_FILE = from(FileTableFunction.PARQUET, "PARQUET_FILE");

    private static final JetSqlOperatorTable INSTANCE = new JetSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private JetSqlOperatorTable() {
    }

    static JetSqlOperatorTable instance() {
        return INSTANCE;
    }

    static SqlUserDefinedTableFunction from(TableFunction function, String name) {
        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;

        List<RelDataType> types = new ArrayList<>();
        List<SqlTypeFamily> families = new ArrayList<>();
        for (FunctionParameter parameter : function.getParameters()) {
            RelDataType type = parameter.getType(typeFactory);
            assert type.getSqlTypeName().getFamily() == CHARACTER;

            types.add(type);
            families.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }
        FamilyOperandTypeChecker typeChecker =
                OperandTypes.family(families, index -> function.getParameters().get(index).isOptional());

        return new JetSqlUserDefinedTableFunction(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                ReturnTypes.CURSOR,
                InferTypes.explicit(types),
                typeChecker,
                types,
                function
        );
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier name,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operators,
            SqlNameMatcher nameMatcher
    ) {
        super.lookupOperatorOverloads(name, category, syntax, operators, SqlNameMatchers.withCaseSensitive(false));
    }

    private static final class JetSqlUserDefinedTableFunction extends SqlUserDefinedTableFunction {

        private JetSqlUserDefinedTableFunction(
                SqlIdentifier opName,
                SqlReturnTypeInference returnTypeInference,
                SqlOperandTypeInference operandTypeInference,
                SqlOperandTypeChecker operandTypeChecker,
                List<RelDataType> paramTypes,
                TableFunction function
        ) {
            super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, function);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory, List<SqlNode> operands) {
            List<Object> arguments = toArguments(getNameAsId(), getFunction().getParameters(), operands);
            return getFunction().getRowType(typeFactory, arguments);
        }

        private static List<Object> toArguments(
                SqlIdentifier name,
                List<FunctionParameter> parameters,
                List<SqlNode> operands
        ) {
            assert parameters.size() == operands.size();

            List<Object> arguments = new ArrayList<>(parameters.size());
            for (int i = 0; i < parameters.size(); i++) {
                SqlNode operand = operands.get(i);
                FunctionParameter parameter = parameters.get(i);
                Object value = extractValue(name, parameter, operand);
                arguments.add(value);
            }
            return arguments;
        }

        private static Object extractValue(
                SqlIdentifier name,
                FunctionParameter parameter,
                SqlNode node
        ) {
            if (node.getKind() == SqlKind.DEFAULT) {
                return null;
            }
            if (SqlUtil.isNullLiteral(node, true)) {
                return null;
            }
            if (SqlUtil.isLiteral(node)) {
                Object value = ((SqlLiteral) node).getValue();
                if (value instanceof NlsString) {
                    return ((NlsString) value).getValue();
                }
            }

            throw QueryException.error("All arguments of call to function "
                    + name + " should be VARCHAR literals. Actual argument #"
                    + parameter.getOrdinal() + " (" + parameter.getName()
                    + ") is not VARCHAR literal: " + node
            );
        }
    }
}
