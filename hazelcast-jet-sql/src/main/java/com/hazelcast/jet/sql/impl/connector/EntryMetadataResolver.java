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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface EntryMetadataResolver {

    String supportedFormat();

    List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    );

    EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    );

    default Map<QueryPath, MappingField> extractKeyFields(
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

    default Map<QueryPath, MappingField> extractValueFields(
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
