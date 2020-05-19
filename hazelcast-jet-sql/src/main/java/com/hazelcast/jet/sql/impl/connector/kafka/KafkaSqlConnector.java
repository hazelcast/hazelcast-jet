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
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlWriters.EntryWriter;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.connector.SqlKeyValueConnector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
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
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.connector.SqlWriters.entryWriter;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.projectionFn;

public class KafkaSqlConnector extends SqlKeyValueConnector implements JetSqlConnector {

    public static final String TYPE_NAME = "com.hazelcast.Kafka";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the map name. If missing, the IMap name is assumed to be equal
     * to the table name.
     */
    public static final String TO_TOPIC_NAME = "mapName";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return false;
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
//        if (!serverOptions.isEmpty()) {
//            throw new JetException("Only local maps are supported for now");
//        }
        String topicName = options.getOrDefault(TO_TOPIC_NAME, tableName);
        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(options);
        kafkaProperties.remove(TO_TOPIC_NAME);
        kafkaProperties.remove(TO_KEY_CLASS);
        kafkaProperties.remove(TO_VALUE_CLASS);
        EntryWriter writer = entryWriter(externalFields, options.get(TO_KEY_CLASS), options.get(TO_VALUE_CLASS));
        return new KafkaTable(this, schemaName, tableName, new ConstantTableStatistics(0), topicName,
                toList(externalFields, TableField::new), writer, kafkaProperties, options);
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
        FunctionEx<ConsumerRecord<Object, Object>, Object[]> mapFn1 = record -> mapFn.apply(entry(record.key(), record.value()));
        Vertex filterVertex = dag.newVertex("kafka-project-filter", mapP(mapFn1));

        dag.edge(between(sourceVertex, filterVertex).isolated());
        return filterVertex;
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

        EntryWriter writer = table.getWriter();
        Vertex vStart = dag.newVertex("kafka-project", mapP(writer));

        String topicName = table.getTopicName();
        Vertex vEnd = dag.newVertex("kafka(" + topicName + ')',
                KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                        table.getKafkaProperties(), topicName, Entry::getKey, Entry::getValue, true));

        dag.edge(between(vStart, vEnd));
        return vStart;
    }
}
