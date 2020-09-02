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

import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;

interface AvroMetadataResolver {

    static void validateFields(List<MappingField> userFields) {
        for (MappingField field : userFields) {
            String path = field.externalName() == null ? field.name() : field.externalName();
            if (path.indexOf('.') >= 0) {
                throw QueryException.error("Invalid field name - '" + path + "'. Nested fields are not supported.");
            }
        }
    }

    static List<MappingField> resolveFieldsFromSchema(Schema schema) {
        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (Schema.Field avroField : schema.getFields()) {
            String name = avroField.name();
            QueryDataType type = resolveType(avroField.schema().getType());

            MappingField field = new MappingField(name, type);

            fields.putIfAbsent(field.name(), field);
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

    static List<TableField> toTableFields(List<MappingField> mappingFields) {
        return toList(mappingFields,
                f -> new FileTableField(f.name(), f.type(), f.externalName() == null ? f.name() : f.externalName()));
    }

    static String[] paths(List<TableField> fields) {
        // TODO: get rid of casting ???
        return fields.stream().map(field -> ((FileTableField) field).getPath()).toArray(String[]::new);
    }

    static QueryDataType[] types(List<TableField> fields) {
        return fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    static Schema schema(List<TableField> fields) {
        String[] paths = paths(fields);
        QueryDataType[] types = types(fields);

        FieldAssembler<Schema> schema = SchemaBuilder.record("name").namespace("namespace").fields(); // TODO:
        for (int i = 0; i < fields.size(); i++) {
            switch (types[i].getTypeFamily()) {
                case BOOLEAN:
                    schema = schema.name(paths[i]).type().booleanType().noDefault();
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    schema = schema.name(paths[i]).type().intType().noDefault();
                    break;
                case BIGINT:
                    schema = schema.name(paths[i]).type().longType().noDefault();
                    break;
                case REAL:
                    schema = schema.name(paths[i]).type().floatType().noDefault();
                    break;
                case DOUBLE:
                    schema = schema.name(paths[i]).type().doubleType().noDefault();
                    break;
                default:
                    schema = schema.name(paths[i]).type().stringType().noDefault();
                    break;
            }
        }
        return schema.endRecord();
    }
}
