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
import com.hazelcast.jet.sql.impl.schema.ExternalField;
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

import static com.hazelcast.jet.sql.SqlConnector.JSON_SERIALIZATION_FORMAT;

// TODO: deduplicate with MapSampleMetadataResolver
final class JsonEntryMetadataResolver implements EntryMetadataResolver {

    static final JsonEntryMetadataResolver INSTANCE = new JsonEntryMetadataResolver();

    private JsonEntryMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return JSON_SERIALIZATION_FORMAT;
    }

    @Override
    public List<ExternalField> resolveFields(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> new QueryPath(name, false));

        if (externalFieldsByPath.isEmpty()) {
            throw QueryException.error("Empty " + (isKey ? "key" : "value") + " column list");
        }

        Map<String, ExternalField> fields = new LinkedHashMap<>();
        for (Entry<QueryPath, ExternalField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            if (path.getPath() == null) {
                throw QueryException.error("Invalid external name '" + path.toString() + "'");
            }
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

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
        Map<QueryPath, ExternalField> externalFieldsByPath = isKey
                ? extractKeyFields(externalFields)
                : extractValueFields(externalFields, name -> new QueryPath(name, false));

        List<TableField> fields = new ArrayList<>();
        //Set<String> pathsRequiringConversion = new HashSet<>();
        for (Entry<QueryPath, ExternalField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();
            //boolean requiresConversion = doesRequireConversion(type);

            MapTableField field = new MapTableField(name, type, false, path/*, requiresConversion*/);

            fields.add(field);
            /*if (field.isRequiringConversion()) {
                pathsRequiringConversion.add(field.getPath().getPath());
            }*/
        }
        return new EntryMetadata(
                new GenericQueryTargetDescriptor(/*pathsRequiringConversion*/),
                HazelcastJsonUpsertTargetDescriptor.INSTANCE,
                fields
        );
    }

    private static boolean doesRequireConversion(QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
            // assuming values are monomorphic
            case BIGINT:
            case VARCHAR:
            //return !type.isStatic();
            default:
                return true;
        }
    }
}
