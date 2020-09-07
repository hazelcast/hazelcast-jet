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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_SERIALIZATION_FORMAT;
import static java.lang.Integer.parseInt;

final class EntryMetadataPortableResolver implements EntryMetadataResolver {

    static final EntryMetadataPortableResolver INSTANCE = new EntryMetadataPortableResolver();

    private EntryMetadataPortableResolver() {
    }

    @Override
    public String supportedFormat() {
        return PORTABLE_SERIALIZATION_FORMAT;
    }

    @Override
    public List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        ClassDefinition classDefinition = resolveClassDefinition(isKey, options, serializationService);
        return resolveFields(isKey, userFields, classDefinition);
    }

    List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            ClassDefinition clazz
    ) {
        Map<QueryPath, MappingField> mappingFieldsByPath = isKey
                ? extractKeyFields(userFields)
                : extractValueFields(userFields, name -> new QueryPath(name, false));

        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (Entry<String, FieldType> entry : resolvePortable(clazz).entrySet()) {
            QueryPath path = new QueryPath(entry.getKey(), isKey);
            QueryDataType type = resolvePortableType(entry.getValue());

            MappingField mappingField = mappingFieldsByPath.get(path);
            if (mappingField != null && !type.getTypeFamily().equals(mappingField.type().getTypeFamily())) {
                throw QueryException.error("Mismatch between declared and inferred type - '" + mappingField.name() + "'");
            }
            String name = mappingField == null ? entry.getKey() : mappingField.name();

            MappingField field = new MappingField(name, type, path.toString());
            fields.putIfAbsent(field.name(), field);
        }
        for (MappingField field : mappingFieldsByPath.values()) {
            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    private static Map<String, FieldType> resolvePortable(ClassDefinition classDefinition) {
        Map<String, FieldType> fields = new LinkedHashMap<>();
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition fieldDefinition = classDefinition.getField(i);
            fields.putIfAbsent(fieldDefinition.getName(), fieldDefinition.getType());
        }
        return fields;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolvePortableType(FieldType type) {
        switch (type) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case BYTE:
                return QueryDataType.TINYINT;
            case SHORT:
                return QueryDataType.SMALLINT;
            case CHAR:
                return QueryDataType.VARCHAR_CHARACTER;
            case UTF:
                return QueryDataType.VARCHAR;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            default:
                return QueryDataType.OBJECT;
        }
    }

    @Override
    public EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> mappingFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        ClassDefinition clazz = resolveClassDefinition(isKey, options, serializationService);
        return resolveMetadata(isKey, mappingFields, clazz);
    }

    EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> mappingFields,
            ClassDefinition clazz
    ) {
        Map<QueryPath, MappingField> mappingFieldsByPath = isKey
                ? extractKeyFields(mappingFields)
                : extractValueFields(mappingFields, name -> new QueryPath(name, false));

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : mappingFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            TableField field = new MapTableField(name, type, false, path);
            fields.add(field);
        }
        return new EntryMetadata(
                GenericQueryTargetDescriptor.DEFAULT,
                new PortableUpsertTargetDescriptor(
                        clazz.getFactoryId(),
                        clazz.getClassId(),
                        clazz.getVersion()
                ),
                fields
        );
    }

    private ClassDefinition resolveClassDefinition(
            boolean isKey,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        String factoryIdProperty = isKey ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID;
        String factoryId = options.get(factoryIdProperty);
        String classIdProperty = isKey ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID;
        String classId = options.get(classIdProperty);
        String classVersionProperty = isKey ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION;
        String classVersion = options.get(classVersionProperty);

        if (factoryId == null || classId == null || classVersion == null) {
            throw QueryException.error(
                    "Unable to resolve table metadata. Missing ['"
                            + factoryIdProperty + "'|'"
                            + classIdProperty + "'|'"
                            + classVersionProperty
                            + "'] option(s)");
        }

        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(parseInt(factoryId), parseInt(classId), parseInt(classVersion));
        if (classDefinition == null) {
            throw QueryException.dataException(
                    "Unable to find class definition for factoryId: " + factoryId
                            + ", classId: " + classId + ", classVersion: " + classVersion
            );
        }
        return classDefinition;
    }
}
