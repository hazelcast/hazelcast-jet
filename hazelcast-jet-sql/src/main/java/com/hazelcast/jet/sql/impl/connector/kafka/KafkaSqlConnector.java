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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlWriters.EntryWriter;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
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
import static com.hazelcast.jet.sql.impl.connector.SqlWriters.entryWriter;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.projectionFn;

public class KafkaSqlConnector implements SqlConnector {

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the map name. If missing, the IMap name is assumed to be equal
     * to the table name.
     */
    public static final String TO_TOPIC_NAME = "mapName";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the key class in the IMap entry. Can be omitted if "__key" is
     * one of the columns.
     */
    public static final String TO_KEY_CLASS = "keyClass";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the value class in the IMap entry. Can be omitted if "this" is
     * one of the columns.
     */
    public static final String TO_VALUE_CLASS = "valueClass";

    private static final ExpressionEvalContext ZERO_ARGUMENTS_CONTEXT = index -> {
        throw new IndexOutOfBoundsException("" + index);
    };

    @Override
    public boolean isStream() {
        return false;
    }

    @Nullable @Override
    public JetTable createTable(
            @Nonnull JetInstance jetInstance,
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions
    ) {
        throw new UnsupportedOperationException("TODO field examination");
    }

    @Nullable @Override
    public JetTable createTable(
            @Nonnull JetInstance jetInstance,
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull List<Entry<String, QueryDataType>> fields
    ) {
        // TODO validate options
//        if (!serverOptions.isEmpty()) {
//            throw new JetException("Only local maps are supported for now");
//        }
        String mapName = tableOptions.getOrDefault(TO_TOPIC_NAME, tableName);
        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(serverOptions);
        kafkaProperties.putAll(tableOptions);
        kafkaProperties.remove(TO_TOPIC_NAME);
        kafkaProperties.remove(TO_KEY_CLASS);
        kafkaProperties.remove(TO_VALUE_CLASS);
        EntryWriter writer = entryWriter(fields, tableOptions.get(TO_KEY_CLASS), tableOptions.get(TO_VALUE_CLASS));
        return new KafkaTable(this, mapName, fields, writer, kafkaProperties);
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nullable @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable,
            @Nullable String timestampField,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        KafkaTable table = (KafkaTable) jetTable;
        String topicName = table.getTopicName();

        FunctionEx<Entry<Object, Object>, Object[]> mapFn = projectionFn(jetTable, predicate, projections);
        FunctionEx<ConsumerRecord<Object, Object>, Object[]> mapFn1 = cr -> mapFn.apply(entry(cr.key(), cr.value()));

        Vertex sourceVertex = dag.newVertex("kafka(" + topicName + ")",
                KafkaProcessors.streamKafkaP(table.getKafkaProperties(), FunctionEx.identity(), noEventTime(), topicName));
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
            @Nonnull JetTable jetTable
    ) {
        KafkaTable table = (KafkaTable) jetTable;

        EntryWriter writer = table.getWriter();
        Vertex vStart = dag.newVertex("project", mapP(writer));

        String topicName = table.getTopicName();
        Vertex vEnd = dag.newVertex("kafka(" + topicName + ')',
                KafkaProcessors.<Entry<Object, Object>, Object, Object>writeKafkaP(
                        table.getKafkaProperties(), topicName, Entry::getKey, Entry::getValue, true));

        dag.edge(between(vStart, vEnd));
        return vStart;
    }
}
