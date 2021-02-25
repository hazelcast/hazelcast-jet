/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Table object for the {@code information_schema.columns} table.
 */
public class MappingColumnsTable extends InfoSchemaTable {

    private static final String NAME = "columns";

    private static final List<TableField> FIELDS = asList(
            new TableField("table_catalog", QueryDataType.VARCHAR, false),
            new TableField("table_schema", QueryDataType.VARCHAR, false),
            new TableField("table_name", QueryDataType.VARCHAR, false),
            new TableField("column_name", QueryDataType.VARCHAR, false),
            new TableField("column_external_name", QueryDataType.VARCHAR, false),
            new TableField("ordinal_position", QueryDataType.INT, false),
            new TableField("is_nullable", QueryDataType.VARCHAR, false),
            new TableField("data_type", QueryDataType.VARCHAR, false)
    );

    private final String catalog;
    private final List<MappingDefinition> definitions;

    public MappingColumnsTable(
            String catalog,
            String schemaName,
            List<MappingDefinition> definitions
    ) {
        super(
                FIELDS,
                schemaName,
                NAME,
                new ConstantTableStatistics((long) definitions.size() * FIELDS.size())
        );

        this.catalog = catalog;
        this.definitions = definitions;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(definitions.size());
        for (MappingDefinition definition : definitions) {
            Map<String, MappingField> mappingFieldsByName = definition.mappingFields().stream()
                    .collect(toMap(MappingField::name, identity()));
            List<TableField> tableFields = definition.tableFields();
            for (int i = 0; i < tableFields.size(); i++) {
                TableField tableField = tableFields.get(i);
                MappingField mappingField = mappingFieldsByName.get(tableField.getName());
                Object[] row = new Object[]{
                        catalog,
                        definition.schema(),
                        definition.name(),
                        tableField.getName(),
                        mappingField == null ? null : mappingField.externalName(),
                        i,
                        String.valueOf(true),
                        tableField.getType().getTypeFamily().name()
                };
                rows.add(row);
            }
        }
        return rows;
    }
}
