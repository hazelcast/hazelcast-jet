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
import com.hazelcast.jet.sql.impl.inject.HazelcastJsonUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
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

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;

final class EntryMetadataHazelcastJsonResolver implements EntryMetadataResolver {

    static final EntryMetadataHazelcastJsonResolver INSTANCE = new EntryMetadataHazelcastJsonResolver();

    private EntryMetadataHazelcastJsonResolver() {
    }

    @Override
    public String supportedFormat() {
        return JSON_SERIALIZATION_FORMAT;
    }

    @Override
    public List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> mappingFieldsByPath = isKey
                ? extractKeyFields(userFields)
                : extractValueFields(userFields, name -> new QueryPath(name, false));

        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (Entry<QueryPath, MappingField> entry : mappingFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            if (path.getPath() == null) {
                throw QueryException.error("Invalid external name '" + path.toString() + "'");
            }
            MappingField field = entry.getValue();

            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    @Override
    public EntryMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> mappingFieldsByPath = isKey
                ? extractKeyFields(resolvedFields)
                : extractValueFields(resolvedFields, name -> new QueryPath(name, false));

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : mappingFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            MapTableField field = new MapTableField(name, type, false, path);
            fields.add(field);
        }
        return new EntryMetadata(
                fields,
                new GenericQueryTargetDescriptor(),
                HazelcastJsonUpsertTargetDescriptor.INSTANCE
        );
    }
}
