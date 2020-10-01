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
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PREFIX;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PREFIX;
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

        // validate the external name: it must be "__key[.*]" or "this[.*]"
        for (MappingField field : userFields) {
            String extName = field.externalName();
            if (extName != null && !extName.equals(KEY) && !extName.equals(VALUE)
                    && !extName.startsWith(KEY_PREFIX) && !extName.startsWith(VALUE_PREFIX)) {
                throw QueryException.error("Invalid external name '" + extName + "'");
            }
        }

        List<MappingField> keyFields = findMetadataResolver(options, true)
                .resolveFields(true, userFields, options, ss);
        List<MappingField> valueFields = findMetadataResolver(options, false)
                .resolveFields(false, userFields, options, ss);

        Map<String, MappingField> fields = concat(keyFields.stream(), valueFields.stream())
                .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        if (fields.isEmpty()) {
            throw QueryException.error("The resolved field list is empty");
        }

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
