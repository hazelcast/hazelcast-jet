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
import com.hazelcast.jet.sql.impl.connector.ResolverUtil;
import com.hazelcast.jet.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.ReflectionUtils.loadClass;
import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PATH;
import static java.util.Collections.singletonList;

// TODO: deduplicate with MapSampleMetadataResolver
public final class JavaEntryMetadataResolver implements EntryMetadataResolver {

    public static final JavaEntryMetadataResolver INSTANCE = new JavaEntryMetadataResolver();

    private JavaEntryMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return JAVA_SERIALIZATION_FORMAT;
    }

    @Override
    public List<ExternalField> resolveFields(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = resolveClass(isKey, options);
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveSchema(externalFields, type, isKey);
        } else {
            return resolveObjectSchema(externalFields, clazz, isKey);
        }
    }

    private List<ExternalField> resolvePrimitiveSchema(
            List<ExternalField> externalFields,
            QueryDataType type,
            boolean isKey
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> VALUE_PATH);

        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;

        ExternalField externalField = externalFieldsByPath.get(path);
        if (externalField != null && !type.getTypeFamily().equals(externalField.type().getTypeFamily())) {
            throw QueryException.error("Mismatch between declared and inferred type - '" + externalField.name() + "'");
        }
        String name = externalField == null ? (isKey ? KEY : VALUE) : externalField.name();

        ExternalField field = new ExternalField(name, type, path.toString());

        for (ExternalField ef : externalFieldsByPath.values()) {
            if (!field.name().equals(ef.name())) {
                throw QueryException.error("Unmapped field - '" + ef.name() + "'");
            }
        }

        return singletonList(field);
    }

    private List<ExternalField> resolveObjectSchema(
            List<ExternalField> externalFields,
            Class<?> clazz,
            boolean isKey
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> new QueryPath(name, false));

        Map<String, ExternalField> fields = new LinkedHashMap<>();
        for (Entry<String, Class<?>> entry : ResolverUtil.resolveClass(clazz).entrySet()) {
            QueryPath path = new QueryPath(entry.getKey(), isKey);
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(entry.getValue());

            ExternalField externalField = externalFieldsByPath.get(path);
            if (externalField != null && !type.getTypeFamily().equals(externalField.type().getTypeFamily())) {
                throw QueryException.error("Mismatch between declared and inferred type - '" + externalField.name() + "'");
            }
            String name = externalField == null ? entry.getKey() : externalField.name();

            ExternalField field = new ExternalField(name, type, path.toString());

            fields.putIfAbsent(field.name(), field);
        }
        for (Entry<QueryPath, ExternalField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            String name = entry.getValue().name();
            QueryDataType type = entry.getValue().type();

            ExternalField field = new ExternalField(name, type, path.toString());

            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    @Override
    public EntryMetadata resolveMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        Class<?> clazz = resolveClass(isKey, options);
        QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);
        if (type != QueryDataType.OBJECT) {
            return resolvePrimitiveMetadata(externalFields, isKey);
        } else {
            return resolveObjectMetadata(externalFields, clazz, isKey);
        }
    }

    private EntryMetadata resolvePrimitiveMetadata(List<ExternalField> externalFields, boolean isKey) {
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> VALUE_PATH);

        QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        ExternalField externalField = externalFieldsByPath.get(path);

        TableField field = new MapTableField(externalField.name(), externalField.type(), false, path);

        return new EntryMetadata(
                GenericQueryTargetDescriptor.DEFAULT,
                PrimitiveUpsertTargetDescriptor.DEFAULT,
                singletonList(field)
        );
    }

    private EntryMetadata resolveObjectMetadata(
            List<ExternalField> externalFields,
            Class<?> clazz,
            boolean isKey
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> new QueryPath(name, false));

        Map<String, Class<?>> typesByNames = ResolverUtil.resolveClass(clazz);

        List<TableField> fields = new ArrayList<>();
        Map<String, String> typeNamesByPaths = new HashMap<>();
        for (Entry<QueryPath, ExternalField> entry : externalFieldsByPath.entrySet()) {
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
                GenericQueryTargetDescriptor.DEFAULT,
                new PojoUpsertTargetDescriptor(clazz.getName(), typeNamesByPaths),
                fields
        );
    }

    private Class<?> resolveClass(boolean isKey, Map<String, String> options) {
        String classNameProperty = isKey ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS;
        String className = options.get(classNameProperty);

        if (className == null) {
            throw QueryException.error("Unable to resolve table metadata. Missing '" + classNameProperty + "' option");
        }

        return loadClass(className);
    }
}
