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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.JavaEntryMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.EntrySqlConnector;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.estimatePartitionedMapRowCount;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapDistributionField;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;

public class LocalPartitionedMapConnector extends EntrySqlConnector {

    public static final String TYPE_NAME = "com.hazelcast.IMap";

    private static final Map<String, EntryMetadataResolver> METADATA_RESOLVERS = Stream.of(
            JavaEntryMetadataResolver.INSTANCE,
            PortableEntryMetadataResolver.INSTANCE,
            JsonEntryMetadataResolver.INSTANCE
    ).collect(toMap(EntryMetadataResolver::supportedFormat, Function.identity()));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Nonnull
    @Override
    public List<ExternalField> resolveFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options
    ) {
        return resolveFields(options, (InternalSerializationService) nodeEngine.getSerializationService());
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> externalFields
    ) {
        String mapName = options.getOrDefault(TO_OBJECT_NAME, name);

        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        EntryMetadata keyMetadata = resolveMetadata(externalFields, options, true, ss);
        EntryMetadata valueMetadata = resolveMetadata(externalFields, options, false, ss);
        List<TableField> fields = mergeFields(keyMetadata.getFields(), valueMetadata.getFields());

        // TODO: deduplicate with PartitionedMapTableResolver ???
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getMapContainer(mapName);

        long estimatedRowCount = estimatePartitionedMapRowCount(nodeEngine, context, mapName);
        List<MapTableIndex> indexes = container != null ? getPartitionedMapIndexes(container, fields) : emptyList();
        int distributionFieldOrdinal =
                container != null ? getPartitionedMapDistributionField(container, context, fields) : -1;
        boolean nativeMemoryEnabled = container != null && nodeEngine.getConfig().getNativeMemoryConfig().isEnabled()
                && container.getMapConfig().getInMemoryFormat() == NATIVE;

        return new PartitionedMapTable(
                schemaName,
                mapName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                indexes,
                distributionFieldOrdinal,
                nativeMemoryEnabled
        );
    }

    @Override
    protected Map<String, EntryMetadataResolver> supportedResolvers() {
        return METADATA_RESOLVERS;
    }
}
