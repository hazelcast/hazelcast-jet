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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.jet.sql.impl.schema.JetTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetTableFunctionParameter;
import com.hazelcast.jet.sql.impl.schema.UnknownStatistic;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.SqlConnector.TO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_CHARSET;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_DELIMITER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_HEADER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_SHARED_FILE_SYSTEM;
import static java.util.Collections.emptyList;

public final class FileTableFunction implements JetTableFunction {

    public static final FileTableFunction INSTANCE = new FileTableFunction();

    // TODO: replace format specific options with map or FILE() -> CSV()/JSON()/AVRO()
    private static final Map<String, FunctionParameter> PARAMETERS_BY_CONNECTOR_NAMES =
            new LinkedHashMap<String, FunctionParameter>() {{
                int ordinal = 0;
                put(TO_SERIALIZATION_FORMAT, new JetTableFunctionParameter(ordinal++, "format", String.class, true));
                put(TO_PATH, new JetTableFunctionParameter(ordinal++, "path", String.class, true));
                put(TO_GLOB, new JetTableFunctionParameter(ordinal++, "glob", String.class, false));
                put(TO_SHARED_FILE_SYSTEM, new JetTableFunctionParameter(ordinal++, "sharedFileSystem", Boolean.class,
                        false));
                put(TO_CHARSET, new JetTableFunctionParameter(ordinal++, "charset", String.class, false));
                put(TO_HEADER, new JetTableFunctionParameter(ordinal++, "header", Boolean.class, false));
                put(TO_DELIMITER, new JetTableFunctionParameter(ordinal, "delimiter", String.class, false));
            }};

    private static final String SCHEMA_NAME_FILES = "files";

    private FileTableFunction() {
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return new ArrayList<>(PARAMETERS_BY_CONNECTOR_NAMES.values());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
        return table(options(arguments), emptyList()).getRowType(typeFactory);
    }

    @Override
    public Type getElementType(List<Object> arguments) {
        return Object[].class;
    }

    @Override
    public HazelcastTable table(List<Object> arguments, RelDataType rowType) {
        Map<String, String> options = options(arguments);

        List<ExternalField> fields = toList(
                rowType.getFieldList(),
                field -> new ExternalField(field.getName(), SqlToQueryType.map(field.getType().getSqlTypeName()), null)
        );

        return table(options, fields);
    }

    private HazelcastTable table(Map<String, String> options, List<ExternalField> fields) {
        Table table = FileSqlConnector.INSTANCE.createTable(
                null,
                SCHEMA_NAME_FILES,
                randomName(),
                options,
                fields
        );
        return new HazelcastTable(table, UnknownStatistic.INSTANCE);
    }

    private Map<String, String> options(List<Object> arguments) {
        Map<String, String> options = new HashMap<>();
        Iterator<String> parameterNames = PARAMETERS_BY_CONNECTOR_NAMES.keySet().iterator();
        for (Object argument : arguments) {
            String parameterName = parameterNames.next();
            if (argument != null) {
                options.put(parameterName, argument.toString());
            }
        }
        return options;
    }

    private static String randomName() {
        return "file_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
