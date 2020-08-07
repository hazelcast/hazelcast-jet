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

import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

interface AvroMetadataResolver {

    static List<TableField> fields(List<ExternalField> externalFields) {
        List<TableField> fields = new ArrayList<>();
        for (ExternalField externalField : externalFields) {
            String name = externalField.name();
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null && externalName.chars().filter(ch -> ch == '.').count() > 0) {
                throw QueryException.error(
                        "Invalid field external name - '" + externalName + "'. Nested fields are not supported."
                );
            }
            String path = externalName == null ? externalField.name() : externalName;

            TableField field = new FileTableField(name, type, path);

            fields.add(field);
        }
        return fields;
    }

    static List<TableField> fields(Schema schema) {
        Map<String, TableField> fields = new LinkedHashMap<>();
        for (Schema.Field avroField : schema.getFields()) {
            String name = avroField.name();
            QueryDataType type = resolveType(avroField.schema().getType());

            TableField field = new FileTableField(name, type);

            fields.putIfAbsent(field.getName(), field);
        }
        return new ArrayList<>(fields.values());
    }

    static QueryDataType resolveType(Schema.Type type) {
        switch (type) {
            case STRING:
                return QueryDataType.VARCHAR;
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            case NULL:
                return QueryDataType.NULL;
            default:
                return QueryDataType.OBJECT;
        }
    }

    static String[] paths(List<TableField> fields) {
        // TODO: get rid of casting ???
        return fields.stream().map(field -> ((FileTableField) field).getPath()).toArray(String[]::new);
    }

    static QueryDataType[] types(List<TableField> fields) {
        return fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }
}
