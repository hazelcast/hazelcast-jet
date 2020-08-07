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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonObject.Member;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

interface JsonMetadataResolver {

    static List<ExternalField> fields(String line) {
        JsonObject object = Json.parse(line).asObject();

        Map<String, ExternalField> fields = new LinkedHashMap<>();
        for (Member member : object) {
            String name = member.getName();
            QueryDataType type = resolveType(member.getValue());

            ExternalField field = new ExternalField(name, type, null);

            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

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

    static QueryDataType resolveType(JsonValue value) {
        if (value.isBoolean()) {
            return QueryDataType.BOOLEAN;
        } else if (value.isNumber()) {
            return QueryDataType.DOUBLE;
        } else if (value.isString()) {
            return QueryDataType.VARCHAR;
        } else {
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
