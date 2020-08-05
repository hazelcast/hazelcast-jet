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
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_CHARSET;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_DELIMITER;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_HEADER;
import static java.nio.charset.StandardCharsets.UTF_8;

final class CsvMetadataResolver {

    private CsvMetadataResolver() {
    }

    static Metadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            String directory,
            String glob
    ) throws IOException {
        String charset = options.getOrDefault(TO_CHARSET, UTF_8.name());
        boolean header = Boolean.parseBoolean(options.getOrDefault(TO_HEADER, "false"));
        String delimiter = options.getOrDefault(TO_DELIMITER, ",");

        return !externalFields.isEmpty()
                ? resolveFromFields(externalFields, charset, header, delimiter)
                : resolveFromSample(directory, glob, charset, delimiter);
    }

    private static Metadata resolveFromFields(
            List<ExternalField> externalFields,
            String charset,
            boolean header,
            String delimiter
    ) {
        List<TableField> fields = new ArrayList<>();
        Map<String, Integer> indicesByNames = new HashMap<>();

        for (int i = 0; i < externalFields.size(); i++) {
            ExternalField externalField = externalFields.get(i);

            String name = externalField.name();
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null) {
                throw QueryException.error("External names are not supported");
            }

            TableField field = new FileTableField(name, type);

            fields.add(field);
            indicesByNames.put(field.getName(), i);
        }

        return new Metadata(
                fields,
                new CsvTargetDescriptor(charset, delimiter, header, indicesByNames)
        );
    }

    private static Metadata resolveFromSample(
            String directory,
            String glob,
            String charset,
            String delimiter
    ) throws IOException {
        String line = line(directory, glob);
        if (line == null) {
            throw new IllegalArgumentException("No data found in '" + directory + "/" + glob + "'");
        }

        Map<String, TableField> fields = new HashMap<>();
        Map<String, Integer> indicesByNames = new HashMap<>();

        String[] headers = line.split(delimiter);
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i];

            TableField field = new FileTableField(header, QueryDataType.VARCHAR);

            if (fields.putIfAbsent(field.getName(), field) == null) {
                indicesByNames.put(field.getName(), i);
            }
        }

        return new Metadata(
                new ArrayList<>(fields.values()),
                new CsvTargetDescriptor(charset, delimiter, true, indicesByNames)
        );
    }

    private static String line(
            String directory,
            String glob
    ) throws IOException {
        for (Path path : Files.newDirectoryStream(Paths.get(directory), glob)) {
            Optional<String> line = Files.lines(path).findFirst();
            if (line.isPresent()) {
                return line.get();
            }
        }
        return null;
    }

    private static final class CsvTargetDescriptor implements TargetDescriptor {

        private final String charset;
        private final String delimiter;
        private final boolean containsHeader;
        private final Map<String, Integer> indicesByNames;

        private CsvTargetDescriptor(
                String charset,
                String delimiter,
                boolean containsHeader,
                Map<String, Integer> indicesByNames
        ) {
            this.charset = charset;
            this.delimiter = delimiter;
            this.containsHeader = containsHeader;
            this.indicesByNames = indicesByNames;
        }

        @Override
        public FunctionEx<? super Path, ? extends Stream<Object[]>> createReader(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            long linesToSkip = this.containsHeader ? 1 : 0;
            String delimiter = this.delimiter;
            Map<String, Integer> indicesByNames = this.indicesByNames;
            String[] paths = fields.stream().map(TableField::getName).toArray(String[]::new);
            QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new CsvQueryTarget(indicesByNames), paths, types, predicate, projection);

            return path -> {
                RowProjector projector = projectorSupplier.get();

                return Files.lines(path, Charset.forName(charset))
                            .skip(linesToSkip)
                            .map(line -> line.split(delimiter))
                            .map(projector::project)
                            .filter(Objects::nonNull);
            };
        }
    }
}
