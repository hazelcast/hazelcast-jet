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
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;

public class EntryMetadataResolvers {

    private final Map<String, EntryMetadataResolver> resolvers;

    public EntryMetadataResolvers(EntryMetadataResolver... resolvers) {
        this.resolvers = stream(resolvers)
                .collect(toMap(EntryMetadataResolver::supportedFormat, Function.identity()));
    }

    public List<MappingField> resolveAndValidateFields(
            List<MappingField> userFields,
            Map<String, String> options,
            NodeEngine nodeEngine
    ) {
        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        List<MappingField> keyFields = findMetadataResolver(options, true)
                .resolveFields(true, userFields, options, ss);
        if (keyFields.isEmpty()) {
            throw QueryException.error("Empty key column list");
        }
        List<MappingField> valueFields = findMetadataResolver(options, false)
                .resolveFields(false, userFields, options, ss);
        if (valueFields.isEmpty()) {
            throw QueryException.error("Empty value column list");
        }

        Map<String, MappingField> fields = concat(keyFields.stream(), valueFields.stream())
                .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        return new ArrayList<>(fields.values());
    }

    public EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        EntryMetadataResolver resolver = findMetadataResolver(options, isKey);
        return requireNonNull(resolver.resolveMetadata(isKey, resolvedFields, options, serializationService));
    }

    private EntryMetadataResolver findMetadataResolver(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT;
        String format = options.get(option);
        if (format == null) {
            throw QueryException.error("Missing '" + option + "' option");
        }
        EntryMetadataResolver resolver = resolvers.get(format);
        if (resolver == null) {
            throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
        return resolver;
    }
}
