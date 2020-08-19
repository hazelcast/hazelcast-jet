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

import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.EntrySqlConnector;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class ReplicatedMapSqlConnector extends EntrySqlConnector {

    public static final String TYPE_NAME = "ReplicatedMap";

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

    @Nonnull @Override
    protected Table createTableInt(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull String objectName,
            @Nonnull Map<String, String> options,
            @Nonnull List<TableField> fields,
            @Nonnull EntryMetadata keyMetadata,
            @Nonnull EntryMetadata valueMetadata
    ) {
        if (!tableName.equals(objectName)) {
            throw QueryException.error("The map name must be equal to the object name");
        }
        // TODO: deduplicate with ReplicatedMapTableResolver ???
        ReplicatedMapService service = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(objectName);

        long estimatedRowCount = stores.size() * nodeEngine.getPartitionService().getPartitionCount();

        return new ReplicatedMapTable(
                schemaName,
                objectName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor()
        );
    }

    @Override
    protected Map<String, EntryMetadataResolver> supportedResolvers() {
        return METADATA_RESOLVERS;
    }
}
