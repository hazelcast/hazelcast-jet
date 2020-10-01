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
import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers.extractKeyFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers.extractValueFields;

public final class KvMetadataAvroResolver implements KvMetadataResolver {

    public static final KvMetadataAvroResolver INSTANCE = new KvMetadataAvroResolver();

    private KvMetadataAvroResolver() {
    }

    @Override
    public String supportedFormat() {
        return AVRO_FORMAT;
    }

    @Override
    public List<MappingField> resolveAndValidateFields(
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

            TableField field = new MapTableField(name, type, false, path);
            fields.add(field);
        }
        return new EntryMetadata(
                fields,
                AvroQueryTargetDescriptor.INSTANCE,
                new AvroUpsertTargetDescriptor(schema(fields).toString())
        );
    }

    private Schema schema(List<TableField> fields) {
        QueryPath[] paths = paths(fields);
        QueryDataType[] types = types(fields);

        FieldAssembler<Schema> schema = SchemaBuilder.record("jet.sql").fields();
        for (int i = 0; i < fields.size(); i++) {
            switch (types[i].getTypeFamily()) {
                case BOOLEAN:
                    schema = schema.name(paths[i].getPath()).type()
                                   .unionOf().nullType().and().booleanType().endUnion()
                                   .nullDefault();
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    schema = schema.name(paths[i].getPath()).type()
                                   .unionOf().nullType().and().intType().endUnion()
                                   .nullDefault();
                    break;
                case BIGINT:
                    schema = schema.name(paths[i].getPath()).type()
                                   .unionOf().nullType().and().longType().endUnion()
                                   .nullDefault();
                    break;
                case REAL:
                    schema = schema.name(paths[i].getPath()).type()
                                   .unionOf().nullType().and().floatType().endUnion()
                                   .nullDefault();
                    break;
                case DOUBLE:
                    schema = schema.name(paths[i].getPath()).type()
                                   .unionOf().nullType().and().doubleType().endUnion()
                                   .nullDefault();
                    break;
                default:
                    schema = schema.name(paths[i].getPath()).type()
                                   .nullable().stringType()
                                   .stringDefault(null);
                    break;
            }
        }
        return schema.endRecord();
    }

    private QueryPath[] paths(List<TableField> fields) {
        return fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
    }

    private QueryDataType[] types(List<TableField> fields) {
        return fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }
}
