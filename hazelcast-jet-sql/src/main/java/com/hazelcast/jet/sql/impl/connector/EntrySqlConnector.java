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
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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

    protected List<ExternalField> resolveSchema(
            List<ExternalField> externalFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        List<ExternalField> keyFields = resolveSchema(externalFields, options, true, serializationService);
        List<ExternalField> valueFields = resolveSchema(externalFields, options, false, serializationService);

        Map<String, ExternalField> fields = Stream.concat(keyFields.stream(), valueFields.stream())
            .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        return new ArrayList<>(fields.values());
    }

    private List<ExternalField> resolveSchema(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String format = resolveFormat(options, isKey);
        EntryMetadataResolver resolver = supportedResolvers().get(format);
        if (resolver == null) {
            throw QueryException.error(format("Unsupported serialization format - '%s'", format));
        }
        return requireNonNull(resolver.resolveSchema(externalFields, options, isKey, serializationService));
    }

    protected EntryMetadata resolveMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String format = resolveFormat(options, isKey);
        EntryMetadataResolver resolver = requireNonNull(supportedResolvers().get(format));
        return requireNonNull(resolver.resolveMetadata(externalFields, options, isKey, serializationService));
    }

    private static String resolveFormat(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_SERIALIZATION_KEY_FORMAT : OPTION_SERIALIZATION_VALUE_FORMAT;
        String format = options.get(option);
        if (format == null) {
            throw QueryException.error(format("Missing '%s' option", option));
        }
        return format;
    }
}
