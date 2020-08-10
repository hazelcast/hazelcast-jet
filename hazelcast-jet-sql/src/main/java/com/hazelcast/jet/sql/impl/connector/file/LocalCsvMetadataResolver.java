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
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.schema;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.fields;

final class LocalCsvMetadataResolver implements CsvMetadataResolver {

    private LocalCsvMetadataResolver() {
    }

    static List<ExternalField> resolveSchema(
            List<ExternalField> externalFields,
            FileOptions options
    ) throws IOException {
        if (!externalFields.isEmpty()) {
            return schema(externalFields);
        } else {
            // TODO: ensure options.header() == true ???
            String line = line(options.path(), options.glob());
            return schema(line, options.delimiter());
        }
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

    static Metadata resolveMetadata(List<ExternalField> externalFields, FileOptions options) {
        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new CsvTargetDescriptor(
                        options.path(),
                        options.glob(),
                        options.sharedFileSystem(),
                        options.charset(),
                        options.delimiter(),
                        options.header()
                ),
                fields
        );
    }

    private static final class CsvTargetDescriptor implements TargetDescriptor {

        private final String path;
        private final String glob;
        private final boolean sharedFileSystem;
        private final String charset;
        private final String delimiter;
        private final boolean header;

        private CsvTargetDescriptor(
                String path,
                String glob,
                boolean sharedFileSystem,
                String charset,
                String delimiter,
                boolean header
        ) {
            this.path = path;
            this.glob = glob;
            this.sharedFileSystem = sharedFileSystem;
            this.charset = charset;
            this.delimiter = delimiter;
            this.header = header;
        }

        @Override
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            long linesToSkip = this.header ? 1 : 0;
            String delimiter = this.delimiter;
            Map<String, Integer> indicesByNames = CsvMetadataResolver.indices(fields);
            String[] paths = CsvMetadataResolver.paths(fields);
            QueryDataType[] types = CsvMetadataResolver.types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new CsvQueryTarget(indicesByNames), paths, types, predicate, projection);

            FunctionEx<? super Path, ? extends Stream<Object[]>> readFileFn = path -> {
                RowProjector projector = projectorSupplier.get();

                return Files.lines(path, Charset.forName(charset))
                            .skip(linesToSkip)
                            .map(line -> line.split(delimiter))
                            .map(projector::project)
                            .filter(Objects::nonNull);
            };

            return ReadFilesP.metaSupplier(path, glob, sharedFileSystem, readFileFn);
        }
    }
}
