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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.connector.SqlKeyValueConnector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadata;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.PojoMapOptionsMetadataResolver;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.sql.impl.connector.SqlProcessors.projectEntrySupplier;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.projectionFn;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

public class KafkaSqlConnector extends SqlKeyValueConnector implements JetSqlConnector {

    public static final String TYPE_NAME = "com.hazelcast.Kafka";

    public static final String TO_TOPIC_NAME = "topicName";

    private static final Map<String, MapOptionsMetadataResolver> METADATA_RESOLVERS = Stream.of(
            new PojoMapOptionsMetadataResolver()
    ).collect(toMap(MapOptionsMetadataResolver::supportedFormat, Function.identity()));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<ExternalField> externalFields,
            @Nonnull Map<String, String> options
    ) {
        // TODO validate options
        String topicName = options.getOrDefault(TO_TOPIC_NAME, tableName);
        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(options);
        kafkaProperties.remove(TO_TOPIC_NAME);
        kafkaProperties.remove(TO_KEY_CLASS);
        kafkaProperties.remove(TO_VALUE_CLASS);

        MapOptionsMetadata keyMetadata = resolveMetadata(externalFields, options, true);
        MapOptionsMetadata valueMetadata = resolveMetadata(externalFields, options, false);
        List<TableField> fields = mergeFields(externalFields, keyMetadata.getFields(), valueMetadata.getFields());

        return new KafkaTable(
                this,
                schemaName,
                tableName,
                new ConstantTableStatistics(0),
                topicName,
                fields,
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                kafkaProperties
        );
    }

    private static MapOptionsMetadata resolveMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean key
    ) {
        String format = options.get(key ? TO_SERIALIZATION_KEY_FORMAT : TO_SERIALIZATION_VALUE_FORMAT);
        if (format == null) {
            return MapOptionsMetadataResolver.resolve(externalFields, key);
        }

        MapOptionsMetadataResolver resolver = METADATA_RESOLVERS.get(format);
        if (resolver == null) {
            throw QueryException.error(
                    format("Specified format '%s' is not among supported ones %s", format, METADATA_RESOLVERS.keySet())
            );
        }

        return checkNotNull(resolver.resolve(externalFields, options, key, null));
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nullable @Override
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

    @Nullable @Override
    public Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table jetTable
    ) {
        KafkaTable table = (KafkaTable) jetTable;

        ProcessorSupplier projectEntrySupplier =
                projectEntrySupplier(table.getKeyUpsertDescriptor(), table.getValueUpsertDescriptor(), table.getFields());
        Vertex vStart = dag.newVertex("kafka-project", projectEntrySupplier);

        String topicName = table.getTopicName();
        Vertex vEnd = dag.newVertex("kafka(" + topicName + ')',
                KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                        table.getKafkaProperties(), topicName, Entry::getKey, Entry::getValue, true));

        dag.edge(between(vStart, vEnd));
        return vStart;
    }
}
