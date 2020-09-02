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

package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public abstract class EntrySqlConnector implements SqlConnector {

    public static final String OPTION_SERIALIZATION_KEY_FORMAT = "serialization.key.format";
    public static final String OPTION_SERIALIZATION_VALUE_FORMAT = "serialization.value.format";

    public static final String OPTION_KEY_CLASS = "serialization.key.java.class";
    public static final String OPTION_VALUE_CLASS = "serialization.value.java.class";

    public static final String OPTION_KEY_FACTORY_ID = "serialization.key.portable.factoryId";
    public static final String OPTION_KEY_CLASS_ID = "serialization.key.portable.classId";
    public static final String OPTION_KEY_CLASS_VERSION = "serialization.key.portable.classVersion";
    public static final String OPTION_VALUE_FACTORY_ID = "serialization.value.portable.factoryId";
    public static final String OPTION_VALUE_CLASS_ID = "serialization.value.portable.classId";
    public static final String OPTION_VALUE_CLASS_VERSION = "serialization.value.portable.classVersion";

    protected abstract Map<String, EntryMetadataResolver> supportedResolvers();

    @Nonnull @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        InternalSerializationService serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        List<MappingField> keyFields = findMetadataResolver(options, true)
                .resolveFields(userFields, options, true, serializationService);
        List<MappingField> valueFields = findMetadataResolver(options, false)
                .resolveFields(userFields, options, false, serializationService);

        assert keyFields != null && valueFields != null;

        Map<String, MappingField> fields = Stream.concat(keyFields.stream(), valueFields.stream())
                                                 .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        return new ArrayList<>(fields.values());
    }

    @Nonnull @Override
    public final Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        String objectName = options.getOrDefault(OPTION_OBJECT_NAME, tableName);

        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        EntryMetadata keyMetadata = resolveMetadata(resolvedFields, options, true, ss);
        EntryMetadata valueMetadata = resolveMetadata(resolvedFields, options, false, ss);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        return createTableInt(nodeEngine, schemaName, tableName, objectName, options, fields, keyMetadata, valueMetadata);
    }

    @Nonnull
    protected abstract Table createTableInt(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull String objectName,
            @Nonnull Map<String, String> options,
            @Nonnull List<TableField> fields,
            @Nonnull EntryMetadata keyMetadata,
            @Nonnull EntryMetadata valueMetadata);

    protected EntryMetadata resolveMetadata(
            List<MappingField> resolvedFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        EntryMetadataResolver resolver = findMetadataResolver(options, isKey);
        return requireNonNull(resolver.resolveMetadata(resolvedFields, options, isKey, serializationService));
    }

    private EntryMetadataResolver findMetadataResolver(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_SERIALIZATION_KEY_FORMAT : OPTION_SERIALIZATION_VALUE_FORMAT;
        String format = options.get(option);
        if (format == null) {
            throw QueryException.error(format("Missing '%s' option", option));
        }
        EntryMetadataResolver resolver = supportedResolvers().get(format);
        if (resolver == null) {
            throw QueryException.error(format("Unsupported serialization format - '%s'", format));
        }
        return resolver;
    }
}
