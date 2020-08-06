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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public final class JetSqlOperatorTable extends ReflectiveSqlOperatorTable {

    @SuppressWarnings("unused")
    public static final SqlFunction FILE = from(FileTableFunction.INSTANCE, "FILE");

    private static final JetSqlOperatorTable INSTANCE = new JetSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private JetSqlOperatorTable() { }

    public static JetSqlOperatorTable instance() {
        return INSTANCE;
    }

    @SuppressWarnings("SameParameterValue")
    private static SqlFunction from(TableFunction function, String name) {
        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;

        List<RelDataType> argTypes = new ArrayList<>();
        List<SqlTypeFamily> typeFamilies = new ArrayList<>();
        for (FunctionParameter parameter : function.getParameters()) {
            RelDataType type = parameter.getType(typeFactory);

            argTypes.add(type);
            typeFamilies.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }
        FamilyOperandTypeChecker typeChecker =
                OperandTypes.family(typeFamilies, index -> function.getParameters().get(index).isOptional());
        List<RelDataType> paramTypes = toSql(argTypes, typeFactory);

        return new SqlUserDefinedTableFunction(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                ReturnTypes.CURSOR,
                InferTypes.explicit(argTypes),
                typeChecker,
                paramTypes,
                function
        );
    }

    private static List<RelDataType> toSql(Collection<RelDataType> types, RelDataTypeFactory typeFactory) {
        return types.stream().map(type -> toSql(type, typeFactory)).collect(Collectors.toList());
    }

    private static RelDataType toSql(RelDataType type, RelDataTypeFactory typeFactory) {
        if (type instanceof RelDataTypeFactoryImpl.JavaType
                && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass() == Object.class) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);
        } else {
            return JavaTypeFactoryImpl.toSql(typeFactory, type);
        }
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher
    ) {
        super.lookupOperatorOverloads(opName, category, syntax, operatorList, SqlNameMatchers.withCaseSensitive(false));
    }
}
