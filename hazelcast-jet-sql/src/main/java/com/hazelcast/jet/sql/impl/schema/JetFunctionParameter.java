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

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

public class JetFunctionParameter implements FunctionParameter {

    private final int ordinal;
    private final String name;
    private final QueryDataType type;
    private final boolean required;

    public JetFunctionParameter(int ordinal, String name, QueryDataType type, boolean required) {
        this.ordinal = ordinal;
        this.name = name;
        this.type = type;
        this.required = required;
    }

    @Override
    public int getOrdinal() {
        return ordinal;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public RelDataType getType(RelDataTypeFactory typeFactory) {
        return typeFactory.createSqlType(SqlToQueryType.map(type.getTypeFamily()));
    }

    @Override
    public boolean isOptional() {
        return !required;
    }
}
