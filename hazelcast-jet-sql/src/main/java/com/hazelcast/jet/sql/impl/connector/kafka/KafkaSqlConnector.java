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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.exec.KeyValueRowExtractor;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

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

    private static final KeyValueRowExtractor KEY_VALUE_ROW_EXTRACTOR = (key, value, path) -> {
        try {
            return PropertyUtils.getProperty(entry(key, value), path); // TODO: custom Row implementation so we don't have to create Entry ???
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw sneakyThrow(e);
        }
    };

    private static final ExpressionEvalContext ZERO_ARGUMENTS_CONTEXT = Collections::emptyList;

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
        String keyClassName = tableOptions.get(TO_KEY_CLASS);
        String valueClassName = tableOptions.get(TO_VALUE_CLASS);
        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(serverOptions);
        kafkaProperties.putAll(tableOptions);
        kafkaProperties.remove(TO_TOPIC_NAME);
        kafkaProperties.remove(TO_KEY_CLASS);
        kafkaProperties.remove(TO_VALUE_CLASS);
        return new KafkaTable(this, mapName, fields, keyClassName, valueClassName, kafkaProperties);
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

        List<QueryDataType> fieldTypes = table.getPhysicalRowType();

        // convert the projection
        List<String> fieldNames = toList(table.getFieldNames(), fieldName -> {
            // convert field name, the property path must start with "key" or "value", we're getting
            // it from a java.util.Map.Entry. Examples:
            //     "__key" -> "key"
            //     "__key.fieldA" -> "key.fieldA"
            //     "fieldB" -> "value.fieldB"
            //     "this" -> "value"
            //     "this.fieldB" -> "value.fieldB"
            if (fieldName.equals(KEY_ATTRIBUTE_NAME.value())) {
                return "key";
            } else if (fieldName.startsWith(KEY_ATTRIBUTE_NAME.value())) {
                return "key." + fieldName.substring(KEY_ATTRIBUTE_NAME.value().length());
            } else if (fieldName.equals(THIS_ATTRIBUTE_NAME.value())) {
                return "value";
            } else if (fieldName.startsWith(THIS_ATTRIBUTE_NAME.value())) {
                return "value." + fieldName.substring(THIS_ATTRIBUTE_NAME.value().length());
            } else {
                return "value." + fieldName;
            }
        });

        @SuppressWarnings("unchecked")
        Expression<Boolean> predicate0 = predicate != null ? predicate
                : (Expression<Boolean>) ConstantExpression.create(QueryDataType.BOOLEAN, true);

        // convert the predicate and filter
        FunctionEx<ConsumerRecord<Object, Object>, Object[]> projectionAndFilter = record -> {
            KeyValueRow row = new KeyValueRow(fieldNames, fieldTypes, KEY_VALUE_ROW_EXTRACTOR);
            row.setKeyValue(record.key(), record.value());

            boolean passed = predicate0.eval(row, ZERO_ARGUMENTS_CONTEXT) == Boolean.TRUE;
            if (!passed) {
                return null;
            }
            Object[] result = new Object[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                result[i] = projections.get(i).eval(row, ZERO_ARGUMENTS_CONTEXT);
            }
            return result;
        };

        Vertex sourceVertex = dag.newVertex("kafka(" + topicName + ")",
                KafkaProcessors.streamKafkaP(table.getKafkaProperties(), FunctionEx.identity(), noEventTime(), topicName));
        Vertex filterVertex = dag.newVertex("kafka-project-filter", mapP(projectionAndFilter));
        dag.edge(between(sourceVertex, filterVertex).isolated());
        return filterVertex;
    }

    @Nullable @Override
    public Tuple2<Vertex, Vertex> nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable,
            @Nonnull RexNode predicateWithParams,
            @Nonnull List<String> projection
    ) {
        return null;
    }

//    @Override
//    public boolean supportsSink() {
//        return true;
//    }
//
//    @Nullable @Override
//    public Vertex sink(
//            @Nonnull DAG dag,
//            @Nonnull JetTable jetTable
//    ) {
//        KafkaTable table = (KafkaTable) jetTable;
//        String mapName = table.getTopicName();
//        String keyClassName = table.getKeyClassName();
//        String valueClassName = table.getValueClassName();
//        if (keyClassName == null && !table.getFieldNames().contains(KEY_ATTRIBUTE_NAME.value())) {
//            throw new JetException("If writing to IMap, you need to either specify " + TO_KEY_CLASS
//                    + " in table options or declare the " + KEY_ATTRIBUTE_NAME.value() + " column");
//        }
//        if (valueClassName == null && !table.getFieldNames().contains(THIS_ATTRIBUTE_NAME.value())) {
//            throw new JetException("If writing to IMap, you need to either specify " + TO_VALUE_CLASS
//                    + " in table options or declare the " + THIS_ATTRIBUTE_NAME.value() + " column");
//        }
//        List<String> fieldNames = table.getFieldNames();
//        List<QueryDataType> fieldTypes = table.getPhysicalRowType();
//
//        // TODO merge projection vertex into the sink vertex
//        int wholeKeyIndex = fieldNames.indexOf(KEY_ATTRIBUTE_NAME.value());
//        int wholeValueIndex = fieldNames.indexOf(THIS_ATTRIBUTE_NAME.value());
//        Vertex vStart = dag.newVertex("project", mapUsingServiceP(
//                ServiceFactories.nonSharedService(pCtx -> new PropertyUtilsBean()),
//                (PropertyUtilsBean bub, Object[] row) -> {
//                    Object key;
//                    if (wholeKeyIndex >= 0) {
//                        key = convert(row[wholeKeyIndex], fieldTypes.get(wholeKeyIndex));
//                    } else {
//                        key = Class.forName(keyClassName).getConstructor().newInstance();
//                    }
//                    Object value;
//                    if (wholeValueIndex >= 0) {
//                        value = convert(row[wholeValueIndex], fieldTypes.get(wholeValueIndex));
//                    } else {
//                        value = Class.forName(valueClassName).getConstructor().newInstance();
//                    }
//                    for (int i = 0; i < fieldNames.size(); i++) {
//                        if (i == wholeKeyIndex || i == wholeValueIndex) {
//                            continue;
//                        }
//                        String fieldName = fieldNames.get(i);
//                        Object o = value;
//                        if (fieldName.startsWith(KEY_ATTRIBUTE_NAME.value() + ".")) {
//                            o = key;
//                            fieldName = fieldName.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
//                        } else if (fieldName.startsWith(THIS_ATTRIBUTE_NAME.value() + ".")) {
//                            fieldName = fieldName.substring(THIS_ATTRIBUTE_NAME.value().length() + 1);
//                        }
//                        PropertyUtils.setProperty(o, fieldName, convert(row[i], fieldTypes.get(i)));
//                    }
//                    return entry(key, value);
//                }));
//        Vertex vEnd = dag.newVertex("mapSink", SinkProcessors.writeMapP(mapName));
//        dag.edge(between(vStart, vEnd));
//        return vStart;
//    }
//
//    private static Object convert(Object v, QueryDataType type) {
//        if (v == null || type.getConverter().getValueClass() == v.getClass()) {
//            return v;
//        }
//        Converter converter = Converters.getConverter(v.getClass());
//        return type.getConverter().convertToSelf(converter, v);
//    }
}
