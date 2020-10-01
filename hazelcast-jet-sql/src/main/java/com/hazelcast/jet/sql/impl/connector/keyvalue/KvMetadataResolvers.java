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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PREFIX;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PREFIX;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;

/**
 * A utility to resolve fields for key-value connectors that support
 * multiple serialization methods.
 */
public class KvMetadataResolvers {

    private final Map<String, KvMetadataResolver> resolvers;

    public KvMetadataResolvers(KvMetadataResolver... resolvers) {
        this.resolvers = stream(resolvers)
                .collect(toMap(KvMetadataResolver::supportedFormat, Function.identity()));
    }

    /**
     * A utility to implement {@link SqlConnector#resolveAndValidateFields} in
     * the connector.
     */
    public List<MappingField> resolveAndValidateFields(
            List<MappingField> userFields,
            Map<String, String> options,
            NodeEngine nodeEngine
    ) {
        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        // validate the external name: it must be "__key[.*]" or "this[.*]"
        for (MappingField field : userFields) {
            String extName = field.externalName();
            if (extName != null && !extName.equals(KEY) && !extName.equals(VALUE)
                    && !extName.startsWith(KEY_PREFIX) && !extName.startsWith(VALUE_PREFIX)) {
                throw QueryException.error("Invalid external name '" + extName + "'");
            }
        }

        List<MappingField> keyFields = findMetadataResolver(options, true)
                .resolveAndValidateFields(true, userFields, options, ss);
        List<MappingField> valueFields = findMetadataResolver(options, false)
                .resolveAndValidateFields(false, userFields, options, ss);

        Map<String, MappingField> fields = concat(keyFields.stream(), valueFields.stream())
                .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        if (fields.isEmpty()) {
            throw QueryException.error("The resolved field list is empty");
        }

        return new ArrayList<>(fields.values());
    }

    /**
     * A utility to implement {@link SqlConnector#createTable} in the
     * connector.
     */
    public EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        KvMetadataResolver resolver = findMetadataResolver(options, isKey);
        return requireNonNull(resolver.resolveMetadata(isKey, resolvedFields, options, serializationService));
    }

    private KvMetadataResolver findMetadataResolver(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT;
        String format = options.get(option);
        if (format == null) {
            throw QueryException.error("Missing '" + option + "' option");
        }
        KvMetadataResolver resolver = resolvers.get(format);
        if (resolver == null) {
            throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
        return resolver;
    }

    public static Map<QueryPath, MappingField> extractKeyFields(
            List<MappingField> fields
    ) {
        Map<QueryPath, MappingField> keyFieldsByPath = new LinkedHashMap<>();
        for (MappingField mappingField : fields) {
            String externalName = mappingField.externalName();

            if (externalName == null
                    || QueryPath.VALUE.equals(externalName)
                    || externalName.startsWith(QueryPath.VALUE_PREFIX)) {
                continue;
            }

            QueryPath path;
            if (QueryPath.KEY.equals(externalName)) {
                path = QueryPath.KEY_PATH;
            } else if (externalName.startsWith(QueryPath.KEY_PREFIX)) {
                path = QueryPath.create(externalName);
            } else {
                throw QueryException.error("Invalid external name: " + externalName);
            }

            if (keyFieldsByPath.putIfAbsent(path, mappingField) != null) {
                throw QueryException.error("Duplicate external name: " + path);
            }
        }
        return keyFieldsByPath;
    }

    public static Map<QueryPath, MappingField> extractValueFields(
            List<MappingField> fields,
            Function<String, QueryPath> defaultPathSupplier
    ) {
        Map<QueryPath, MappingField> valueFieldsByPath = new LinkedHashMap<>();
        for (MappingField mappingField : fields) {
            String externalName = mappingField.externalName();

            if (externalName != null
                    && (QueryPath.KEY.equals(externalName) || externalName.startsWith(QueryPath.KEY_PREFIX))) {
                continue;
            }

            QueryPath path;
            if (externalName == null) {
                path = defaultPathSupplier.apply(mappingField.name());
            } else if (QueryPath.VALUE.equals(externalName)) {
                path = QueryPath.VALUE_PATH;
            } else if (externalName.startsWith(QueryPath.VALUE_PREFIX)) {
                path = QueryPath.create(externalName);
            } else {
                throw QueryException.error("Invalid external name: " + externalName);
            }

            if (valueFieldsByPath.putIfAbsent(path, mappingField) != null) {
                throw QueryException.error("Duplicate external name: " + path);
            }
        }
        return valueFieldsByPath;
    }
}
