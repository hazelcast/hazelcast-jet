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
import com.hazelcast.jet.sql.impl.schema.JetFunctionParameter;
import com.hazelcast.jet.sql.impl.schema.UnknownStatistic;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.SqlConnector.OPTION_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_CHARSET;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_DELIMITER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_HEADER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_S3_ACCESS_KEY;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_S3_SECRET_KEY;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.util.Arrays.asList;

public final class FileTableFunction implements JetTableFunction {

    public static final FileTableFunction INSTANCE = new FileTableFunction();

    // TODO: replace format-specific options with map or FILE() -> CSV()/JSON()/AVRO() ?
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final List<Pair<String, FunctionParameter>> PARAMETERS = asList(
            Pair.of(OPTION_SERIALIZATION_FORMAT, new JetFunctionParameter(0, "format", String.class, true)),
            Pair.of(OPTION_PATH, new JetFunctionParameter(1, "path", String.class, true)),
            Pair.of(OPTION_GLOB, new JetFunctionParameter(2, "glob", String.class, false)),
            Pair.of(OPTION_SHARED_FILE_SYSTEM, new JetFunctionParameter(3, "sharedFileSystem", Boolean.class, false)),
            Pair.of(OPTION_CHARSET, new JetFunctionParameter(4, "charset", String.class, false)),
            Pair.of(OPTION_HEADER, new JetFunctionParameter(5, "header", Boolean.class, false)),
            Pair.of(OPTION_DELIMITER, new JetFunctionParameter(6, "delimiter", String.class, false)),

            // TODO: cloud credentials as map ?
            Pair.of(OPTION_S3_ACCESS_KEY, new JetFunctionParameter(7, "s3AccessKey", String.class, false)),
            Pair.of(OPTION_S3_SECRET_KEY, new JetFunctionParameter(8, "s3SecretKey", String.class, false))
    );

    private static final String SCHEMA_NAME_FILES = "files";

    private FileTableFunction() {
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return Pair.right(PARAMETERS);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
        Map<String, String> options = optionsFromFunctionArguments(arguments);
        List<ExternalField> fields = FileSqlConnector.resolveAndValidateFields(options);
        return createHazelcastTable(options, fields).getRowType(typeFactory);
    }

    @Override
    public Type getElementType(List<Object> arguments) {
        return Object[].class;
    }

    @Override
    public HazelcastTable table(List<Object> arguments, RelDataType rowType) {
        Map<String, String> options = optionsFromFunctionArguments(arguments);

        List<ExternalField> fields = toList(
                rowType.getFieldList(),
                field -> new ExternalField(field.getName(), SqlToQueryType.map(field.getType().getSqlTypeName()))
        );

        return createHazelcastTable(options, fields);
    }

    private HazelcastTable createHazelcastTable(Map<String, String> options, List<ExternalField> fields) {
        Table table = FileSqlConnector.createTable(
                SCHEMA_NAME_FILES,
                randomName(),
                options,
                fields
        );
        return new HazelcastTable(table, UnknownStatistic.INSTANCE);
    }

    /**
     * Takes a list of function arguments and converts it to equivalent options
     * that would be used if the file was declared using DDL.
     */
    private Map<String, String> optionsFromFunctionArguments(List<Object> arguments) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < PARAMETERS.size(); i++) {
            if (arguments.get(i) != null) {
                options.put(PARAMETERS.get(i).left, arguments.get(i).toString());
            }
        }
        return options;
    }

    private static String randomName() {
        return "file_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
