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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJsonResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataNullResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class KafkaSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "Kafka";

    private final KvMetadataResolvers metadataResolvers;

    public KafkaSqlConnector() {
        this.metadataResolvers = new KvMetadataResolvers(
                new KvMetadataResolver[]{
                        KvMetadataNullResolver.INSTANCE,
                        KvMetadataJavaResolver.INSTANCE,
                        KvMetadataJsonResolver.INSTANCE,
                        KvMetadataAvroResolver.INSTANCE
                },
                new KvMetadataResolver[]{
                        KvMetadataJavaResolver.INSTANCE,
                        KvMetadataJsonResolver.INSTANCE,
                        KvMetadataAvroResolver.INSTANCE
                }
        );
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Nonnull @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        return metadataResolvers.resolveAndValidateFields(userFields, options, nodeEngine);
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        String topicName = options.getOrDefault(OPTION_OBJECT_NAME, tableName);

        KvMetadata keyMetadata = metadataResolvers.resolveMetadata(true, resolvedFields, options, null);
        KvMetadata valueMetadata = metadataResolvers.resolveMetadata(false, resolvedFields, options, null);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        return new KafkaTable(
                this,
                schemaName,
                tableName,
                fields,
                new ConstantTableStatistics(0),
                topicName,
                PropertiesResolver.resolveProperties(options),
                keyMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor()
        );
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nonnull @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        KafkaTable table = (KafkaTable) table0;

        Vertex vStart = dag.newVertex(
                table.toString(),
                KafkaProcessors.streamKafkaP(
                        table.kafkaProperties(),
                        record -> entry(record.key(), record.value()),
                        noEventTime(),
                        table.topicName()
                )
        );

        Vertex vEnd = dag.newVertex(
                "Project(" + table.toString() + ")",
                KvProcessors.rowProjector(
                        table.paths(),
                        table.types(),
                        table.keyQueryDescriptor(),
                        table.valueQueryDescriptor(),
                        predicate,
                        projections
                )
        );

        dag.edge(between(vStart, vEnd).isolated());
        return vEnd;
    }

    @Override
    public boolean supportsSink() {
        return true;
    }

    @Override
    public boolean supportsInsert() {
        return true;
    }

    @Nonnull @Override
    public Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table0
    ) {
        KafkaTable table = (KafkaTable) table0;

        Vertex vStart = dag.newVertex(
                "Project(" + table.toString() + ")",
                KvProcessors.entryProjector(
                        table.paths(),
                        table.types(),
                        table.keyUpsertDescriptor(),
                        table.valueUpsertDescriptor()
                )
        );

        Vertex vEnd = dag.newVertex(
                table.toString(),
                KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                        table.kafkaProperties(),
                        table.topicName(),
                        Entry::getKey,
                        Entry::getValue,
                        true
                )
        );

        dag.edge(between(vStart, vEnd));
        return vStart;
    }
}
