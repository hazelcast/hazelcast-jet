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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.jet.sql.impl.expression.ExpressionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.connector.SqlKeyValueConnector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadataResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.sql.impl.connector.SqlProcessors.entryProjectorProcessorSupplier;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.joinFn;
import static com.hazelcast.jet.sql.impl.expression.ExpressionUtil.projectionFn;

// TODO remove this class in favor of imdg implementation
public class IMapSqlConnector extends SqlKeyValueConnector implements JetSqlConnector {

    @Override
    public String typeName() {
        return "imap-tmp";
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
            @Nonnull Map<String, String> options,
            @Nullable List<ExternalField> externalFields
    ) {
        throw new RuntimeException("should not be used");
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
        PartitionedMapTable table = (PartitionedMapTable) table0;

        FunctionEx<Entry<Object, Object>, Object[]> mapProjection =
                ExpressionUtil.projectionFn(table, predicate, projections);

        String mapName = table.getName();
        return dag.newVertex("map(" + mapName + ")",
                readMapP(mapName, Predicates.alwaysTrue(), mapProjection::apply));
    }

    @Override
    public boolean supportsNestedLoopReader() {
        return true;
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

    @Override
    public boolean supportsSink() {
        return true;
    }

    @Override
    public boolean supportsPlainInserts() {
        return false;
    }

    @Nullable @Override
    public Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table0
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        ProcessorSupplier projectorProcessorSupplier =
                entryProjectorProcessorSupplier(table.getKeyUpsertDescriptor(), table.getValueUpsertDescriptor(), table.getFields());
        Vertex vStart = dag.newVertex("map-project", projectorProcessorSupplier);

        String mapName = table.getName();
        Vertex vEnd = dag.newVertex("map(" + mapName + ")", SinkProcessors.writeMapP(mapName));

        dag.edge(between(vStart, vEnd));
        return vStart;
    }

    @Override
    protected Map<String, MapOptionsMetadataResolver> supportedResolvers() {
        throw new UnsupportedOperationException();
    }
}
