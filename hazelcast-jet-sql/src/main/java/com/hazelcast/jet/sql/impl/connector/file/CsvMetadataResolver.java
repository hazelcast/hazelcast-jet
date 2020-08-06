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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.impl.ReadHadoopNewApiP;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.hadoop.impl.SerializableConfiguration.asSerializable;
import static java.util.stream.Collectors.toMap;

final class CsvMetadataResolver {

    private CsvMetadataResolver() {
    }

    static Metadata resolve(List<ExternalField> externalFields, FileOptions options) throws IOException {
        return !externalFields.isEmpty()
                ? resolveFromFields(externalFields, options)
                : resolveFromSample(options);
    }

    private static Metadata resolveFromFields(List<ExternalField> externalFields, FileOptions options) {
        String path = options.path();
        String glob = options.glob();
        boolean sharedFileSystem = options.sharedFileSystem();
        String charset = options.charset();
        String delimiter = options.delimiter();
        boolean header = options.header();

        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new CsvTargetDescriptor(path, glob, sharedFileSystem, charset, delimiter, header),
                fields
        );
    }

    private static Metadata resolveFromSample(FileOptions options) throws IOException {
        String path = options.path();
        String glob = options.glob();
        boolean sharedFileSystem = options.sharedFileSystem();
        String charset = options.charset();
        String delimiter = options.delimiter();

        String line = line(path, glob);
        if (line == null) {
            throw new IllegalArgumentException("No data found in '" + path + "/" + glob + "'");
        }

        Map<String, TableField> fields = new HashMap<>();

        String[] headers = line.split(delimiter);
        for (String header : headers) {
            TableField field = new FileTableField(header, QueryDataType.VARCHAR);

            fields.putIfAbsent(field.getName(), field);
        }

        return new Metadata(
                new CsvTargetDescriptor(path, glob, sharedFileSystem, charset, delimiter, true),
                new ArrayList<>(fields.values())
        );
    }

    private static String line(String directory, String glob) throws IOException {
        for (Path path : Files.newDirectoryStream(Paths.get(directory), glob)) {
            Optional<String> line = Files.lines(path).findFirst();
            if (line.isPresent()) {
                return line.get();
            }
        }
        return null;
    }

    static Metadata resolve(List<ExternalField> externalFields, FileOptions options, Job job) throws IOException {
        // TODO: resolve from sample
        if (externalFields.isEmpty()) {
            throw QueryException.error("Empty column list");
        }

        String path = options.path();
        String delimiter = options.delimiter();

        TextInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(path));
        job.setInputFormatClass(TextInputFormat.class);

        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new HadoopCsvTargetDescriptor(delimiter, job.getConfiguration()),
                fields
        );
    }

    private static List<TableField> fields(List<ExternalField> externalFields) {
        List<TableField> fields = new ArrayList<>();
        for (ExternalField externalField : externalFields) {
            String name = externalField.name();
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null) {
                throw QueryException.error("External names are not supported");
            }

            TableField field = new FileTableField(name, type);

            fields.add(field);
        }
        return fields;
    }

    private static Map<String, Integer> indices(List<TableField> fields) {
        return IntStream.range(0, fields.size()).boxed().collect(toMap(i -> fields.get(i).getName(), i -> i));
    }

    private static String[] paths(List<TableField> fields) {
        return fields.stream().map(TableField::getName).toArray(String[]::new);
    }

    private static QueryDataType[] types(List<TableField> fields) {
        return fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    private static final class CsvTargetDescriptor implements TargetDescriptor {

        private final String path;
        private final String glob;
        private final boolean sharedFileSystem;
        private final String charset;
        private final String delimiter;
        private final boolean containsHeader;

        private CsvTargetDescriptor(
                String path,
                String glob,
                boolean sharedFileSystem,
                String charset,
                String delimiter,
                boolean containsHeader
        ) {
            this.path = path;
            this.glob = glob;
            this.sharedFileSystem = sharedFileSystem;
            this.charset = charset;
            this.delimiter = delimiter;
            this.containsHeader = containsHeader;
        }

        @Override
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            long linesToSkip = this.containsHeader ? 1 : 0;
            String delimiter = this.delimiter;
            Map<String, Integer> indicesByNames = indices(fields);
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

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

    private static class HadoopCsvTargetDescriptor implements TargetDescriptor {

        private final String delimiter;
        private final Configuration configuration;

        private HadoopCsvTargetDescriptor(
                String delimiter,
                Configuration configuration
        ) {
            this.delimiter = delimiter;
            this.configuration = configuration;
        }

        @Override
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String delimiter = this.delimiter;
            Map<String, Integer> indicesByNames = indices(fields);
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new CsvQueryTarget(indicesByNames), paths, types, predicate, projection);

            SupplierEx<BiFunctionEx<Object, Object, Object[]>> projectionSupplierFn = () -> {
                RowProjector projector = projectorSupplier.get();
                return (position, line) -> projector.project(line.toString().split(delimiter));
            };

            return new ReadHadoopNewApiP.MetaSupplier<>(asSerializable(configuration), projectionSupplierFn);
        }
    }
}
