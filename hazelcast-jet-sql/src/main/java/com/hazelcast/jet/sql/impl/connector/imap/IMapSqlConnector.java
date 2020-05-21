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

package com.hazelcast.jet.sql.impl.connector.imap;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlWriters.EntryWriter;
import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.connector.SqlKeyValueConnector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.connector.SqlWriters.entryWriter;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.joinFn;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.projectionFn;

// TODO remove this class in favor of imdg implementation
public class IMapSqlConnector extends SqlKeyValueConnector implements JetSqlConnector {

    @Override
    public boolean isStream() {
        return false;
    }

    @Override
    public String typeName() {
        return "imap-tmp";
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<ExternalField> externalFields,
            @Nonnull Map<String, String> options
    ) {
        throw new RuntimeException("should not be used");
//        // TODO validate options
//        // if (!serverOptions.isEmpty()) {
//        //     throw new JetException("Only local maps are supported for now");
//        // }
//        String mapName = options.getOrDefault(TO_MAP_NAME, tableName);
//        EntryWriter writer = entryWriter(fields, options.get(TO_KEY_CLASS), options.get(TO_VALUE_CLASS));
//        return new IMapTable(this, mapName,
//                toList(fields, TableField::new),
//                schemaName, tableName, new ConstantTableStatistics(0), writer);
    }

    @Nullable @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable String timestampField,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        FunctionEx<Entry<Object, Object>, Object[]> mapProjection =
                ExpressionUtil.projectionFn(table, predicate, projections);

        String mapName = table.getName();
        return dag.newVertex("map(" + mapName + ")",
                readMapP(mapName, Predicates.alwaysTrue(), mapProjection::apply));
    }

    @Nullable @Override
    public Vertex nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections,
            @Nonnull Expression<Boolean> joinPredicate
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        FunctionEx<Entry<Object, Object>, Object[]> mapFn = projectionFn(table, predicate, projections);
        BiFunctionEx<Object[], Object[], Object[]> joinFn = joinFn(joinPredicate);
        BiFunctionEx<IMap<Object, Object>, Object[], Traverser<Object[]>> flatMapFn =
                (IMap<Object, Object> map, Object[] left) -> {
                    List<Object[]> result = new ArrayList<>();
                    for (Entry<Object, Object> entry : map.entrySet()) {
                        Object[] right = mapFn.apply(entry);
                        // TODO: support LEFT OUTER JOIN ??? connector should not be aware of type of the join though ???
                        if (right != null) {
                            Object[] joined = joinFn.apply(left, right);
                            if (joined != null) {
                                result.add(joined);
                            }
                        }
                    }
                    return traverseIterable(result);
                };

        String mapName = table.getName();
        return dag.newVertex("map-enrich-" + UuidUtil.newUnsecureUuidString(),
                flatMapUsingServiceP(ServiceFactories.iMapService(mapName), flatMapFn));
    }

    @Nullable @Override
    public Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table0
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        String keyClass = table.getDdlOptions().get(TO_KEY_CLASS);
        String valueClass = table.getDdlOptions().get(TO_VALUE_CLASS);

        EntryWriter writer = entryWriter(toList(table.getFields(), this::toExternalField), keyClass, valueClass);

        Vertex vStart = dag.newVertex("map-project", mapP(writer));

        String mapName = table.getName();
        Vertex vEnd = dag.newVertex("map(" + mapName + ")", SinkProcessors.writeMapP(mapName));

        dag.edge(between(vStart, vEnd));
        return vStart;
    }

    private ExternalField toExternalField(TableField t) {
        return new ExternalField(t.getName(), t.getType());
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

    @Override
    public boolean supportsPlainInserts() {
        return false;
    }
}
