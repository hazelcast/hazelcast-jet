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
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers.maybeAddDefaultField;
import static java.lang.Integer.parseInt;

final class MetadataPortableResolver implements KvMetadataResolver {

    static final MetadataPortableResolver INSTANCE = new MetadataPortableResolver();

    private MetadataPortableResolver() {
    }

    @Override
    public String supportedFormat() {
        return PORTABLE_FORMAT;
    }

    @Override
    public List<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        ClassDefinition classDefinition = resolveClassDefinition(isKey, options, serializationService, userFields);
        return resolveFields(isKey, userFields, classDefinition);
    }

    List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            ClassDefinition clazz
    ) {
        Set<Entry<String, FieldType>> fieldsInClass = resolvePortable(clazz).entrySet();

        Map<QueryPath, MappingField> userFieldsByPath = extractFields(userFields, isKey);

        if (!userFields.isEmpty()) {
            // the user used explicit fields in the DDL, just validate them
            for (Entry<String, FieldType> classField : fieldsInClass) {
                QueryPath path = new QueryPath(classField.getKey(), isKey);
                QueryDataType type = resolvePortableType(classField.getValue());

                MappingField mappingField = userFieldsByPath.get(path);
                if (mappingField != null && !type.getTypeFamily().equals(mappingField.type().getTypeFamily())) {
                    throw QueryException.error("Mismatch between declared and resolved type: " + mappingField.name());
                }
            }
            return new ArrayList<>(userFieldsByPath.values());
        } else {
            List<MappingField> fields = new ArrayList<>();
            for (Entry<String, FieldType> classField : fieldsInClass) {
                QueryPath path = new QueryPath(classField.getKey(), isKey);
                QueryDataType type = resolvePortableType(classField.getValue());
                String name = classField.getKey();

                fields.add(new MappingField(name, type, path.toString()));
            }
            return fields;
        }
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
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        ClassDefinition clazz = resolveClassDefinition(isKey, options, serializationService, resolvedFields);
        return resolveMetadata(isKey, resolvedFields, clazz);
    }

    KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            ClassDefinition clazz
    ) {
        Map<QueryPath, MappingField> externalFieldsByPath = extractFields(resolvedFields, isKey);

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }

        maybeAddDefaultField(isKey, externalFieldsByPath, fields);
        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new PortableUpsertTargetDescriptor(clazz)
        );
    }

    private ClassDefinition resolveClassDefinition(
            boolean isKey,
            Map<String, String> options,
            InternalSerializationService serializationService,
            List<MappingField> userFields) {
        String factoryIdProperty = isKey ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID;
        String factoryIdStr = options.get(factoryIdProperty);
        String classIdProperty = isKey ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID;
        String classIdStr = options.get(classIdProperty);
        String versionProperty = isKey ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION;
        String versionStr = options.getOrDefault(versionProperty, "0");

        if (factoryIdStr == null || classIdStr == null || versionStr == null) {
            throw QueryException.error(
                    "Unable to resolve table metadata. Missing ['"
                            + factoryIdProperty + "'|'"
                            + classIdProperty + "'|'"
                            + versionProperty
                            + "'] option(s)");
        }

        int factoryId = parseInt(factoryIdStr);
        int classId = parseInt(classIdStr);
        int version = parseInt(versionStr);
        PortableContext portableContext = serializationService.getPortableContext();
        ClassDefinition classDefinition = portableContext.lookupClassDefinition(factoryId, classId, version);

        if (classDefinition == null) {
            ClassDefinitionBuilder builder = new ClassDefinitionBuilder(factoryId, classId, version);
            for (MappingField field : userFields) {
                QueryDataTypeFamily typeFamily = field.type().getTypeFamily();
                switch (typeFamily) {
                    case VARCHAR:
                        builder.addUTFField(field.name());
                        break;
                    case BOOLEAN:
                        builder.addBooleanField(field.name());
                        break;
                    case TINYINT:
                        builder.addByteField(field.name());
                        break;
                    case SMALLINT:
                        builder.addShortField(field.name());
                        break;
                    case INTEGER:
                        builder.addIntField(field.name());
                        break;
                    case REAL:
                        builder.addFloatField(field.name());
                        break;
                    case DOUBLE:
                        builder.addDoubleField(field.name());
                        break;
                    case DECIMAL:
                    case BIGINT:
                    case NULL:
                    case TIME:
                    case DATE:
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_TIME_ZONE:
                    case OBJECT:
                    default:
                        throw QueryException.error(
                                "The type " + typeFamily + " is not supported for Portable. "
                                        + "Can not create class definition factoryId: " + factoryId
                                        + " classId: " + classId + ", version: " + version);
                }
            }
            classDefinition = portableContext.registerClassDefinition(builder.build());
        }
        return classDefinition;
    }
}
