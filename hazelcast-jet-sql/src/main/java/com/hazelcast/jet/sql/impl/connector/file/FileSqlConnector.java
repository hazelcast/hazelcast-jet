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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class FileSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "File";

    public static final String OPTION_PATH = "path";
    public static final String OPTION_GLOB = "glob";
    public static final String OPTION_SHARED_FILE_SYSTEM = "sharedFileSystem";
    public static final String OPTION_CHARSET = "charset";
    public static final String OPTION_HEADER = "header";
    public static final String OPTION_DELIMITER = "delimiter";

    //public static final String OPTION_S3_ACCESS_KEY = "file.s3a.access.key";
    //public static final String OPTION_S3_SECRET_KEY = "file.s3a.secret.key";

    static final FileSqlConnector INSTANCE = new FileSqlConnector();

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
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        return resolveAndValidateFields(options, userFields);
    }

    @Nonnull
    static List<MappingField> resolveAndValidateFields(
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        return MetadataResolver.resolveAndValidateFields(userFields, FileOptions.from(options));
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        return createTable(schemaName, name, options, resolvedFields);
    }

    @Nonnull
    static Table createTable(
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        Metadata metadata = MetadataResolver.resolveMetadata(resolvedFields, FileOptions.from(options));

        return new FileTable(
                INSTANCE,
                schemaName,
                name,
                metadata.fields(),
                metadata.targetDescriptor()
        );
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        FileTable table = (FileTable) table0;

        return dag.newVertex(table.toString(), table.readProcessor(predicate, projection));
    }
}
