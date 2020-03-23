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

package com.hazelcast.jet.sql.imap;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.schema.JetTable;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.PropertyUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.Util.getRequiredTableOption;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

public class IMapSqlConnector implements SqlConnector {

    public static final String TO_MAP_NAME = "mapName";
    public static final String TO_KEY_CLASS = "keyClass";
    public static final String TO_VALUE_CLASS = "valueClass";

    @Override
    public boolean isStream() {
        return false;
    }

    @Nullable @Override
    public JetTable createTable(
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions
    ) {
        throw new UnsupportedOperationException("TODO field examination");
    }

    @Nullable @Override
    public JetTable createTable(
            @Nonnull String tableName,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull List<Entry<String, QueryDataType>> fields
    ) {
        // TODO validate options
//        if (!serverOptions.isEmpty()) {
//            throw new JetException("Only local maps are supported for now");
//        }
        String mapName = getRequiredTableOption(tableOptions, TO_MAP_NAME);
        List<HazelcastTableIndex> indexes = Collections.emptyList(); // TODO
        return new IMapTable(this, mapName, indexes, fields);
    }

    @Nullable @Override
    public Tuple2<Vertex, Vertex> fullScanReader(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable,
            @Nullable String timestampField,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Integer> projection
    ) {
        IMapTable table = (IMapTable) jetTable;
        String mapName = table.getMapName();

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
        int[] projection0 = projection.stream().mapToInt(i -> i).toArray();
        Projection<Entry<Object, Object>, Object[]> mapProjection = entry -> {
            // TODO use Extractors
            Object[] res = new Object[projection0.length];

            for (int i = 0; i < projection0.length; i++) {
                int fieldIndex = projection0[i];
                try {
                    res[i] = PropertyUtils.getProperty(entry, fieldNames.get(fieldIndex));
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            }
            return res;
        };

        // convert the predicate
        // TODO make the ReadMapOrCacheP.LocalProcessorMetaSupplier implement IdentifiedDataSerializable
        Predicate<Object, Object> mapPredicate;
        if (predicate == null) {
            mapPredicate = Predicates.alwaysTrue();
        } else {
            List<QueryDataType> fieldTypes = table.getPhysicalRowType();
            mapPredicate = entry -> {
                KeyValueRow row = new KeyValueRow(fieldNames, fieldTypes, (key, val, path) ->
                {
                    try {
                        return PropertyUtils.getProperty(entry(key, val), path);
                    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        throw sneakyThrow(e);
                    }
                });
                row.setKeyValue(entry.getKey(), entry.getValue());
                boolean res = predicate.eval(row) == Boolean.TRUE;
                System.out.println("evaluated " + entry + " to " + res);
                return res;
            };
        }

        Vertex vRead = dag.newVertex("map(" + mapName + ")",
                readMapP(mapName, mapPredicate, mapProjection));
        return tuple2(vRead, vRead);
    }

    @Nullable @Override
    public Tuple2<Vertex, Vertex> nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull JetTable jetTable,
            @Nonnull RexNode predicateWithParams,
            @Nonnull List<String> projection
    ) {
        IMapTable table = (IMapTable) jetTable;
        String mapName = table.getMapName();

        Vertex v = dag.newVertex("enrich", Processors.mapUsingServiceAsyncP(
                ServiceFactories.iMapService(mapName),
                1024,
                true,
                t -> {
                    throw new RuntimeException();
                }, // not needed for ordered
                (map, item) -> {
                    // TODO query the map based on the predicate&projection
                    return null;
                }
        ));
        return tuple2(v, v);
    }

    @Nullable @Override
    public Tuple2<Vertex, Vertex> sink(
            @Nonnull DAG dag,
            @Nonnull Map<String, String> serverOptions,
            @Nonnull Map<String, String> tableOptions,
            @Nonnull List<String> fields
    ) {
        String mapName = tableOptions.get(TO_MAP_NAME);
        String keyClassName = tableOptions.get(TO_KEY_CLASS);
        String valueClassName = tableOptions.get(TO_VALUE_CLASS);
        if (keyClassName == null || valueClassName == null) {
            throw new JetException("If writing to IMap, you need to specify " + TO_KEY_CLASS + " and "
                    + TO_VALUE_CLASS + " in table options");
        }
        if (!serverOptions.isEmpty()) {
            throw new JetException("Only local maps are supported for now");
        }
        Vertex vStart = dag.newVertex("project", Processors.<Object[], Entry<Object, Object>>mapP(row -> {
            BeanUtilsBean bub = new BeanUtilsBean();
            Object key = Class.forName(keyClassName).getConstructor().newInstance();
            Object value = Class.forName(valueClassName).getConstructor().newInstance();
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                Object o = value;
                if (field.startsWith(KEY_ATTRIBUTE_NAME.value())) {
                    o = key;
                    field = field.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
                }
                bub.setProperty(o, field, row[i]);
            }
            return entry(key, value);
        }));
        Vertex vEnd = dag.newVertex("mapSink", SinkProcessors.writeMapP(mapName));
        return tuple2(vStart, vEnd);
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Override
    public boolean supportsNestedLoopReader() {
        return true;
    }

    @Override
    public boolean supportsSink() {
        return true;
    }
}
