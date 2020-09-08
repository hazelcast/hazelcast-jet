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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataResolvers;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.sql.impl.connector.EntryProcessors.entryProjector;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.projectionFn;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class KafkaSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "Kafka";

    private final EntryMetadataResolvers entryMetadataResolvers;

    public KafkaSqlConnector() {
        this.entryMetadataResolvers = new EntryMetadataResolvers(
                EntryMetadataJavaResolver.INSTANCE
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
        return entryMetadataResolvers.resolveAndValidateFields(userFields, options, nodeEngine);
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        String objectName = options.getOrDefault(OPTION_OBJECT_NAME, tableName);

        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        EntryMetadata keyMetadata = entryMetadataResolvers.resolveMetadata(true, resolvedFields, options, ss);
        EntryMetadata valueMetadata = entryMetadataResolvers.resolveMetadata(false, resolvedFields, options, ss);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(options);
        kafkaProperties.remove(OPTION_OBJECT_NAME);
        kafkaProperties.remove(OPTION_KEY_CLASS);
        kafkaProperties.remove(OPTION_VALUE_CLASS);

        return new KafkaTable(
                this,
                schemaName,
                objectName,
                new ConstantTableStatistics(0),
                objectName,
                fields,
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                kafkaProperties
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
            @Nullable String timestampField,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        KafkaTable table = (KafkaTable) table0;

        String topicName = table.getTopicName();
        Vertex sourceVertex = dag.newVertex("kafka(" + topicName + ")",
                KafkaProcessors.streamKafkaP(table.getKafkaProperties(), FunctionEx.identity(), noEventTime(), topicName));

        // TODO: use descriptors
        FunctionEx<Entry<Object, Object>, Object[]> mapFn = projectionFn(table, predicate, projections);
        FunctionEx<ConsumerRecord<Object, Object>, Object[]> wrapToEntryFn = record ->
                mapFn.apply(entry(record.key(), record.value()));
        Vertex filterProjectVertex = dag.newVertex("kafka-filter-project", mapP(wrapToEntryFn));

        dag.edge(between(sourceVertex, filterProjectVertex).isolated());
        return filterProjectVertex;
    }

    @Override
    public boolean supportsSink() {
        return true;
    }

    @Nonnull @Override
    public Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table jetTable
    ) {
        KafkaTable table = (KafkaTable) jetTable;

        Vertex vStart = dag.newVertex(
                "kafka-project",
                entryProjector(table.getKeyUpsertDescriptor(), table.getValueUpsertDescriptor(), table.getFields())
        );

        String topicName = table.getTopicName();
        Vertex vEnd = dag.newVertex("kafka(" + topicName + ')',
                KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                        table.getKafkaProperties(), topicName, Entry::getKey, Entry::getValue, true));

        dag.edge(between(vStart, vEnd));
        return vStart;
    }
}
