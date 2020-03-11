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
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.schema.JetTable;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.beanutils.BeanUtilsBean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.sql.Util.getRequiredTableOption;
import static com.hazelcast.projection.Projections.identity;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;

public class IMapSqlConnector implements SqlConnector {

    public static final String TO_MAP_NAME = "com.hazelcast.map.name";
    public static final String TO_KEY_CLASS = "com.hazelcast.map.keyClass";
    public static final String TO_VALUE_CLASS = "com.hazelcast.map.valueClass";

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
            @Nonnull List<Entry<String, RelProtoDataType>> fields
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
            @Nonnull RexNode predicate,
            @Nonnull List<Integer> projection
    ) {
        IMapTable table = (IMapTable) jetTable;
        String mapName = table.getMapName();

        // convert the predicate
        Predicate<Object, Object> mapPredicate;
        if (predicate.isAlwaysFalse()) {
            // TODO don't create noop vertex, eliminate in optimization
            Vertex v = dag.newVertex("noop-src", Processors.noopP());
            return tuple2(v, v);
        } else if (predicate.isAlwaysTrue()) {
            mapPredicate = Predicates.alwaysTrue();
        } else {
            mapPredicate = Predicates.alwaysTrue(); // TODO
        }

//        // convert the projection
//        // TODO don't project if we select all fields
//        Projection<Entry<Object, Object>, Object[]> mapProjection = entry -> {
//            // TODO use Extractors
//            Object[] res = new Object[projection.size()];
//            BeanUtilsBean bub = new BeanUtilsBean();
//
//            for (int i = 0; i < projection.size(); i++) {
//                int fieldIndex = projection.get(i);
//                try {
//                    res[i] = bub.getProperty(entry, table.getField(fieldIndex));
//                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            return res;
//        };

        Projection<Entry<Object, Object>, ?> mapProjection = identity();
        Vertex v = dag.newVertex("map(" + mapName + ")", SourceProcessors.readMapP(mapName, mapPredicate, mapProjection));
        return tuple2(v, v);
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
