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
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PATH;
import static java.util.Collections.singletonList;

final class MetadataJavaResolver implements EntryMetadataResolver {

    public static final MetadataJavaResolver INSTANCE = new MetadataJavaResolver();

    private MetadataJavaResolver() {
    }

    @Override
    public String supportedFormat() {
        return JAVA_FORMAT;
    }

    @Override
    public List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = loadClass(isKey, options);
        return resolveFields(isKey, userFields, clazz);
    }

    List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Class<?> clazz
    ) {
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveSchema(isKey, userFields, type);
        } else {
            return resolveObjectSchema(isKey, userFields, clazz);
        }
    }

    private List<MappingField> resolvePrimitiveSchema(
            boolean isKey,
            List<MappingField> userFields,
            QueryDataType type
    ) {
        Map<QueryPath, MappingField> userFieldsByPath = isKey
                ? extractKeyFields(userFields)
                : extractValueFields(userFields, name -> VALUE_PATH);

        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;

        MappingField mappingField = userFieldsByPath.get(path);
        if (mappingField != null && !type.getTypeFamily().equals(mappingField.type().getTypeFamily())) {
            throw QueryException.error("Mismatch between declared and inferred type for field '"
                    + mappingField.name() + "'");
        }
        String name = mappingField == null ? (isKey ? KEY : VALUE) : mappingField.name();

        MappingField field = new MappingField(name, type, path.toString());

        for (MappingField mf : userFieldsByPath.values()) {
            if (!field.name().equals(mf.name())) {
                throw QueryException.error("Unmapped field: " + mf.name());
            }
        }

        return singletonList(field);
    }

    private List<MappingField> resolveObjectSchema(
            boolean isKey,
            List<MappingField> userFields,
            Class<?> clazz
    ) {
        Set<Entry<String, Class<?>>> fieldsInClass = ReflectionUtils.extractProperties(clazz).entrySet();

        Map<QueryPath, MappingField> userFieldsByPath = isKey
                ? extractKeyFields(userFields)
                : extractValueFields(userFields, name -> new QueryPath(name, false));

        if (!userFields.isEmpty()) {
            // the user used explicit fields in the DDL, just validate them
            for (Entry<String, Class<?>> classField : fieldsInClass) {
                QueryPath path = new QueryPath(classField.getKey(), isKey);
                QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(classField.getValue());

                MappingField mappingField = userFieldsByPath.get(path);
                if (mappingField != null && !type.getTypeFamily().equals(mappingField.type().getTypeFamily())) {
                    throw QueryException.error("Mismatch between declared and inferred type for field '"
                            + mappingField.name() + "'");
                }
            }
            return new ArrayList<>(userFieldsByPath.values());
        } else {
            List<MappingField> fields = new ArrayList<>();
            for (Entry<String, Class<?>> classField : fieldsInClass) {
                QueryPath path = new QueryPath(classField.getKey(), isKey);
                QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(classField.getValue());
                String name = classField.getKey();
                fields.add(new MappingField(name, type, path.toString()));
            }
            return fields;
        }
    }

    @Override
    public EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = loadClass(isKey, options);
        return resolveMetadata(isKey, resolvedFields, clazz);
    }

    EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Class<?> clazz
    ) {
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveMetadata(isKey, resolvedFields);
        } else {
            return resolveObjectMetadata(isKey, resolvedFields, clazz);
        }
    }

    private EntryMetadata resolvePrimitiveMetadata(boolean isKey, List<MappingField> resolvedFields) {
        Map<QueryPath, MappingField> externalFieldsByPath = isKey
                ? extractKeyFields(resolvedFields)
                : extractValueFields(resolvedFields, name -> VALUE_PATH);

        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        MappingField mappingField = externalFieldsByPath.get(path);

        TableField field = new MapTableField(mappingField.name(), mappingField.type(), false, path);

        return new EntryMetadata(
                singletonList(field),
                GenericQueryTargetDescriptor.DEFAULT,
                PrimitiveUpsertTargetDescriptor.INSTANCE
        );
    }

    private EntryMetadata resolveObjectMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Class<?> clazz
    ) {
        Map<QueryPath, MappingField> externalFieldsByPath = isKey
                ? extractKeyFields(resolvedFields)
                : extractValueFields(resolvedFields, name -> new QueryPath(name, false));

        Map<String, Class<?>> typesByNames = ReflectionUtils.extractProperties(clazz);

        List<TableField> fields = new ArrayList<>();
        Map<String, String> typeNamesByPaths = new HashMap<>();
        for (Entry<QueryPath, MappingField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            TableField field = new MapTableField(name, type, false, path);
            fields.add(field);
            if (typesByNames.get(path.getPath()) != null) {
                typeNamesByPaths.put(path.getPath(), typesByNames.get(path.getPath()).getName());
            }
        }
        return new EntryMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new PojoUpsertTargetDescriptor(clazz.getName(), typeNamesByPaths)
        );
    }

    private Class<?> loadClass(boolean isKey, Map<String, String> options) {
        String classNameProperty = isKey ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS;
        String className = options.get(classNameProperty);

        if (className == null) {
            throw QueryException.error("Unable to resolve table metadata. Missing '" + classNameProperty + "' option");
        }

        return ReflectionUtils.loadClass(className);
    }
}
