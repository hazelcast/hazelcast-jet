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
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.hadoop.impl.SerializableConfiguration.asSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

final class AvroMetadataResolver {

    private AvroMetadataResolver() {
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

        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new AvroTargetDescriptor(path, glob, sharedFileSystem),
                fields
        );
    }

    private static Metadata resolveFromSample(FileOptions options) throws IOException {
        String path = options.path();
        String glob = options.glob();
        boolean sharedFileSystem = options.sharedFileSystem();

        Schema schema = schema(path, glob);
        if (schema == null) {
            throw new IllegalArgumentException("No data found in '" + path + "/" + glob + "'");
        }

        Map<String, TableField> fields = new HashMap<>();

        for (Schema.Field avroField : schema.getFields()) {
            String name = avroField.name();
            QueryDataType type = resolveType(avroField.schema().getType());

            TableField field = new FileTableField(name, type);

            fields.putIfAbsent(field.getName(), field);
        }

        return new Metadata(
                new AvroTargetDescriptor(path, glob, sharedFileSystem),
                new ArrayList<>(fields.values())
        );
    }

    private static Schema schema(String directory, String glob) throws IOException {
        for (Path path : Files.newDirectoryStream(Paths.get(directory), glob)) {
            File file = path.toFile();

            DataFileReader<GenericRecord> reader = new DataFileReader<>(file, new GenericDatumReader<>());
            return reader.getSchema();
        }
        return null;
    }

    private static QueryDataType resolveType(Schema.Type avroType) {
        switch (avroType) {
            case STRING:
                return QueryDataType.VARCHAR;
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            case NULL:
                return QueryDataType.NULL;
            default:
                return QueryDataType.OBJECT;
        }
    }

    static Metadata resolve(List<ExternalField> externalFields, FileOptions options, Job job) throws IOException {
        // TODO: resolve from sample
        if (externalFields.isEmpty()) {
            throw QueryException.error("Empty column list");
        }

        String path = options.path();

        AvroKeyInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(path));
        job.setInputFormatClass(AvroKeyInputFormat.class);

        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new HadoopAvroTargetDescriptor(job.getConfiguration()),
                fields
        );
    }

    private static List<TableField> fields(List<ExternalField> externalFields) {
        List<TableField> fields = new ArrayList<>();
        for (ExternalField externalField : externalFields) {
            String name = externalField.name();
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null && externalName.chars().filter(ch -> ch == '.').count() > 0) {
                throw QueryException.error(
                        "Invalid field external name - '" + externalName + "'. Nested fields are not supported."
                );
            }
            String filePath = externalName == null ? externalField.name() : externalName;

            TableField field = new FileTableField(name, type, filePath);

            fields.add(field);
        }
        return fields;
    }

    private static String[] paths(List<TableField> fields) {
        // TODO: get rid of casting ???
        return fields.stream().map(field -> ((FileTableField) field).getPath()).toArray(String[]::new);
    }

    private static QueryDataType[] types(List<TableField> fields) {
        return fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    private static class AvroTargetDescriptor implements TargetDescriptor {

        private final String path;
        private final String glob;
        private final boolean sharedFileSystem;

        private AvroTargetDescriptor(
                String path,
                String glob,
                boolean sharedFileSystem
        ) {
            this.path = path;
            this.glob = glob;
            this.sharedFileSystem = sharedFileSystem;
        }

        @Override
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new AvroQueryTarget(), paths, types, predicate, projection);

            FunctionEx<? super Path, ? extends Stream<Object[]>> readFileFn = path -> {
                RowProjector projector = projectorSupplier.get();

                DataFileReader<GenericRecord> reader = new DataFileReader<>(path.toFile(), new GenericDatumReader<>());
                return StreamSupport.stream(reader.spliterator(), false)
                                    .map(projector::project)
                                    .filter(Objects::nonNull)
                                    .onClose(() -> uncheckRun(reader::close));
            };

            return ReadFilesP.metaSupplier(path, glob, sharedFileSystem, readFileFn);
        }
    }

    private static class HadoopAvroTargetDescriptor implements TargetDescriptor {

        private final Configuration configuration;

        private HadoopAvroTargetDescriptor(
                Configuration configuration
        ) {
            this.configuration = configuration;
        }

        @Override
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new AvroQueryTarget(), paths, types, predicate, projection);

            SupplierEx<BiFunctionEx<AvroKey<GenericRecord>, NullWritable, Object[]>> projectionSupplierFn = () -> {
                RowProjector projector = projectorSupplier.get();
                return (key, value) -> projector.project(key.datum());
            };

            return new ReadHadoopNewApiP.MetaSupplier<>(asSerializable(configuration), projectionSupplierFn);
        }
    }
}
