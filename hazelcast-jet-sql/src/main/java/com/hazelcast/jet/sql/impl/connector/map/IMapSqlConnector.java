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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.estimatePartitionedMapRowCount;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class IMapSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "IMap";

    private final KvMetadataResolvers metadataResolvers;

    public IMapSqlConnector() {
        this.metadataResolvers = new KvMetadataResolvers(
                KvMetadataJavaResolver.INSTANCE,
                MetadataPortableResolver.INSTANCE,
                MetadataJsonResolver.INSTANCE
        );
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return false;
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
    public final Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        String mapName = options.getOrDefault(OPTION_OBJECT_NAME, tableName);

        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        KvMetadata keyMetadata = metadataResolvers.resolveMetadata(true, resolvedFields, options, ss);
        KvMetadata valueMetadata = metadataResolvers.resolveMetadata(false, resolvedFields, options, ss);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getMapContainer(mapName);

        long estimatedRowCount = estimatePartitionedMapRowCount(nodeEngine, context, mapName);
        boolean hd = container != null && container.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE;

        return new PartitionedMapTable(
                schemaName,
                tableName,
                mapName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                Collections.emptyList(),
                hd
        );
    }

    @Override
    public boolean supportsNestedLoopReader() {
        return true;
    }

    @Nonnull @Override
    public NestedLoopJoin nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections,
            @Nonnull JetJoinInfo joinInfo
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        String name = table.getMapName();
        List<TableField> fields = table.getFields();
        QueryPath[] paths = fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
        QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
        QueryTargetDescriptor keyDescriptor = table.getKeyDescriptor();
        QueryTargetDescriptor valueDescriptor = table.getValueDescriptor();

        KvRowProjector.Supplier rightRowProjectorSupplier =
                KvRowProjector.supplier(paths, types, keyDescriptor, valueDescriptor, predicate, projections);

        int leftEquiJoinPrimitiveKeyIndex = leftEquiJoinPrimitiveKeyIndex(joinInfo, fields);
        if (leftEquiJoinPrimitiveKeyIndex > -1) {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Lookup-" + toString(table) + ")",
                            new JoinByPrimitiveKeyProcessorSupplier(
                                    joinInfo.isInner(),
                                    leftEquiJoinPrimitiveKeyIndex,
                                    joinInfo.condition(),
                                    name,
                                    rightRowProjectorSupplier
                            )
                    ),
                    edge -> edge.partitioned(extractPrimitiveKeyFn(leftEquiJoinPrimitiveKeyIndex)).distributed()
            );
        } else if (joinInfo.isEquiJoin() && joinInfo.isInner()) {
            // TODO: define new edge type (mix of broadcast & local-round-robin) ?
            Vertex ingress = dag
                    .newUniqueVertex("Broadcast", () -> new TransformP<>(Traversers::singleton))
                    .localParallelism(1);

            Vertex egress = dag.newUniqueVertex(
                    "Join(Predicate-" + toString(table) + ")",
                    JoinByPredicateInnerProcessorSupplier.supplier(joinInfo, name, paths, rightRowProjectorSupplier)
            );

            dag.edge(between(ingress, egress).partitioned(wholeItem()));

            return new NestedLoopJoin(ingress, egress, edge -> edge.distributed().broadcast());
        } else if (joinInfo.isEquiJoin() && joinInfo.isOuter()) {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Predicate-" + toString(table) + ")",
                            new JoinByPredicateOuterProcessorSupplier(joinInfo, name, paths, rightRowProjectorSupplier)
                    )
            );
        } else {
            return new NestedLoopJoin(
                    dag.newUniqueVertex(
                            "Join(Scan-" + toString(table) + ")",
                            new JoinScanProcessorSupplier(joinInfo, name, rightRowProjectorSupplier)
                    )
            );
        }
        // TODO: detect and handle always-false condition ?
    }

    private static int leftEquiJoinPrimitiveKeyIndex(JetJoinInfo joinInfo, List<TableField> fields) {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int i = 0; i < rightEquiJoinIndices.length; i++) {
            MapTableField field = (MapTableField) fields.get(rightEquiJoinIndices[i]);
            QueryPath path = field.getPath();
            if (path.isTop() && path.isKey()) {
                return joinInfo.leftEquiJoinIndices()[i];
            }
        }
        return -1;
    }

    private static FunctionEx<Object, ?> extractPrimitiveKeyFn(int index) {
        return row -> {
            Object value = ((Object[]) row)[index];
            return value == null ? "" : value;
        };
    }

    @Override
    public boolean supportsSink() {
        return true;
    }

    @Override
    public boolean supportsInsert() {
        return false;
    }

    @Nonnull @Override
    public Vertex sink(
            @Nonnull DAG dag,
            @Nonnull Table table0
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        List<TableField> fields = table.getFields();
        QueryPath[] paths = fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
        QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);

        Vertex vStart = dag.newUniqueVertex(
                "Project(" + toString(table) + ")",
                KvProcessors.entryProjector(
                        paths,
                        types,
                        (UpsertTargetDescriptor) table.getKeyJetMetadata(),
                        (UpsertTargetDescriptor) table.getValueJetMetadata()
                )
        );

        Vertex vEnd = dag.newUniqueVertex(
                toString(table),
                SinkProcessors.writeMapP(table.getMapName())
        );

        dag.edge(between(vStart, vEnd));
        return vStart;
    }

    private static String toString(PartitionedMapTable table) {
        return TYPE_NAME + "[" + table.getSchemaName() + "." + table.getSqlName() + "]";
    }
}
