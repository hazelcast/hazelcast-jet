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
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.fields;

final class LocalJsonMetadataResolver implements JsonMetadataResolver {

    private LocalJsonMetadataResolver() {
    }

    static Metadata resolve(List<ExternalField> externalFields, FileOptions options) throws IOException {
        return !externalFields.isEmpty()
                ? resolveFromFields(externalFields, options)
                : resolveFromSample(options);
    }

    private static Metadata resolveFromFields(List<ExternalField> externalFields, FileOptions options) {
        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new JsonTargetDescriptor(options.path(), options.glob(), options.sharedFileSystem(), options.charset()),
                fields
        );
    }

    private static Metadata resolveFromSample(FileOptions options) throws IOException {
        String path = options.path();
        String glob = options.glob();

        String line = line(path, glob);
        List<TableField> fields = fields(line);

        return new Metadata(
                new JsonTargetDescriptor(path, glob, options.sharedFileSystem(), options.charset()),
                fields
        );
    }

    private static String line(String directory, String glob) throws IOException {
        for (Path path : Files.newDirectoryStream(Paths.get(directory), glob)) { // TODO: directory check
            Optional<String> line = Files.lines(path).findFirst();
            if (line.isPresent()) {
                return line.get();
            }
        }
        throw new IllegalArgumentException("No data found in '" + directory + "/" + glob + "'");
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
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            String[] paths = JsonMetadataResolver.paths(fields);
            QueryDataType[] types = JsonMetadataResolver.types(fields);

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
    }
}
