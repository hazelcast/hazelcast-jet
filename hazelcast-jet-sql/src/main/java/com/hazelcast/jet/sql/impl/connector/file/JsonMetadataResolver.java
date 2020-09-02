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
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;

interface JsonMetadataResolver {

    /**
     * Validates the field list. Returns a field list that has non-null
     * externalName for each field.
     */
    static void validateFields(List<MappingField> userFields) {
        for (MappingField field : userFields) {
            String path = field.externalName() == null ? field.name() : field.externalName();
            if (path.indexOf('.') >= 0) {
                throw QueryException.error("Invalid field name - '" + path + "'. Nested fields are not supported.");
            }
        }
    }

    static List<MappingField> resolveFieldsFromSample(String line) {
        JsonObject object = Json.parse(line).asObject();

        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (Member member : object) {
            String name = member.getName();
            QueryDataType type = resolveType(member.getValue());

            MappingField field = new MappingField(name, type);

            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    static List<TableField> toTableFields(List<MappingField> mappingFields) {
        return toList(mappingFields,
                f -> new FileTableField(f.name(), f.type(), f.externalName() == null ? f.name() : f.externalName()));
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
