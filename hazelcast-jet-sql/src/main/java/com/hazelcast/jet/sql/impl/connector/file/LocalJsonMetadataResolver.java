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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.WriteFileP;
import com.hazelcast.jet.sql.impl.connector.Processors;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.jet.sql.impl.inject.JsonUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.impl.util.Util.firstLineFromFirstFile;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.paths;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.resolveFieldsFromSample;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.toTableFields;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.types;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.validateFields;

final class LocalJsonMetadataResolver implements JsonMetadataResolver {

    private LocalJsonMetadataResolver() {
    }

    static List<ExternalField> resolveFields(
            List<ExternalField> userFields,
            FileOptions options
    ) throws IOException {
        if (!userFields.isEmpty()) {
            validateFields(userFields);
            return userFields;
        } else {
            String line = firstLineFromFirstFile(options.path(), options.glob());
            if (line == null) {
                throw new IllegalArgumentException("No data found in '" + options.path() + "/" + options.glob() + "'");
            }
            return resolveFieldsFromSample(line);
        }
    }

    static Metadata resolveMetadata(List<ExternalField> externalFields, FileOptions options) {
        List<TableField> fields = toTableFields(externalFields);

        return new Metadata(
                new JsonTargetDescriptor(options.path(), options.glob(), options.sharedFileSystem(), options.charset()),
                fields
        );
    }

    private static final class JsonTargetDescriptor implements TargetDescriptor {

        private final String path;
        private final String glob;
        private final boolean sharedFileSystem;
        private final String charset;

        private JsonTargetDescriptor(
                String path,
                String glob,
                boolean sharedFileSystem,
                String charset
        ) {
            this.path = path;
            this.glob = glob;
            this.sharedFileSystem = sharedFileSystem;
            this.charset = charset;
        }

        @Override
        public ProcessorMetaSupplier readProcessor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new JsonQueryTarget(), paths, types, predicate, projection);

            FunctionEx<? super Path, ? extends Stream<Object[]>> readFileFn = path -> {
                RowProjector projector = projectorSupplier.get();

                return Files.lines(path, Charset.forName(charset))
                            .map(projector::project)
                            .filter(Objects::nonNull);
            };

            return ReadFilesP.metaSupplier(path, glob, sharedFileSystem, readFileFn);
        }

        @Override
        public ProcessorSupplier projectorProcessor(List<TableField> fields) {
            return Processors.projector(JsonUpsertTargetDescriptor.INSTANCE, paths(fields), types(fields));
        }

        @Override
        public ProcessorMetaSupplier writeProcessor(List<TableField> fields) {
            // TODO: customizable settings
            return WriteFileP.metaSupplier(path, identity(), charset, null, 1024, true);
        }
    }
}
