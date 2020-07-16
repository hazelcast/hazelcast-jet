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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.sql.JetSqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

public class FileSqlConnector implements JetSqlConnector {

    static final FileSqlConnector INSTANCE = new FileSqlConnector();

    public static final String TYPE_NAME = "com.hazelcast.File";

    public static final String TO_DIRECTORY = "file.directory";
    public static final String TO_GLOB = "file.glob";
    public static final String TO_SHARED_FILE_SYSTEM = "file.sharedFileSystem";

    public static final String TO_CHARSET = "file.charset";
    public static final String TO_HEADER = "file.header";
    public static final String TO_DELIMITER = "file.delimiter";

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
    public Table createTable(
            @Nullable NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> externalFields
    ) {
        String directory = options.get(TO_DIRECTORY);
        if (directory == null) {
            throw QueryException.error(format("Missing '%s' option", TO_DIRECTORY));
        }
        String glob = options.getOrDefault(TO_GLOB, "*");
        boolean sharedFileSystem = parseBoolean(options.getOrDefault(TO_SHARED_FILE_SYSTEM, "true"));
        Metadata metadata = MetadataResolver.resolve(externalFields, options, directory, glob);

        return new FileTable(
                this,
                schemaName,
                name,
                directory,
                glob,
                sharedFileSystem,
                metadata.getFields(),
                metadata.getTargetDescriptor()
        );
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nullable
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable String timestampField,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        FileTable table = (FileTable) table0;

        String directory = table.getDirectory();
        String glob = table.getGlob();
        boolean sharedFileSystem = table.isSharedFileSystem();
        FunctionEx<? super Path, ? extends Stream<Object[]>> readerFn =
                table.getTargetDescriptor().createReader(table.getFields(), predicate, projection);

        return dag.newVertex(
                "file(" + directory + ")",
                ReadFilesP.metaSupplier(directory, glob, sharedFileSystem, readerFn)
        );
    }
}
