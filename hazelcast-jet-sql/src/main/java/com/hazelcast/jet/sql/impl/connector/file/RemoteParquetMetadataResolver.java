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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.HadoopProcessors;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

import static com.hazelcast.jet.hadoop.impl.SerializableConfiguration.asSerializable;
import static com.hazelcast.jet.sql.impl.connector.file.ParquetMetadataResolver.paths;
import static com.hazelcast.jet.sql.impl.connector.file.ParquetMetadataResolver.resolveFieldsFromSchema;
import static com.hazelcast.jet.sql.impl.connector.file.ParquetMetadataResolver.toTableFields;
import static com.hazelcast.jet.sql.impl.connector.file.ParquetMetadataResolver.types;
import static com.hazelcast.jet.sql.impl.connector.file.ParquetMetadataResolver.validateFields;

final class RemoteParquetMetadataResolver implements ParquetMetadataResolver {

    private RemoteParquetMetadataResolver() {
    }

    static List<MappingField> resolveFields(
            List<MappingField> userFields,
            FileOptions options,
            Job job
    ) throws IOException {
        if (!userFields.isEmpty()) {
            validateFields(userFields);
            return userFields;
        } else {
            Schema schema = findParquetSchema(options.path(), job.getConfiguration());
            return resolveFieldsFromSchema(schema);
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    private static Schema findParquetSchema(String directory, Configuration configuration) throws IOException {
        Path path = new Path(directory);
        try (FileSystem filesystem = path.getFileSystem(configuration)) {
            // TODO: directory check, recursive ???
            RemoteIterator<LocatedFileStatus> filesIterator = filesystem.listFiles(path, false);
            if (filesIterator.hasNext()) {
                LocatedFileStatus file = filesIterator.next();

                try (
                        ParquetReader<GenericRecord> reader = AvroParquetReader
                                .<GenericRecord>builder(HadoopInputFile.fromPath(file.getPath(), configuration))
                                .build()
                ) {
                    GenericRecord record = reader.read();
                    if (record != null) {
                        return record.getSchema();
                    }
                }
            }
        }
        throw new IllegalArgumentException("No data found in '" + directory + "'");
    }

    static Metadata resolveMetadata(List<MappingField> mappingFields, FileOptions options, Job job) throws IOException {
        List<TableField> fields = toTableFields(mappingFields);

        AvroParquetInputFormat.addInputPath(job, new Path(options.path()));
        job.setInputFormatClass(AvroParquetInputFormat.class);

        return new Metadata(
                new ParquetTargetDescriptor(job.getConfiguration()),
                fields
        );
    }

    private static final class ParquetTargetDescriptor implements TargetDescriptor {

        private final Configuration configuration;

        private ParquetTargetDescriptor(
                Configuration configuration
        ) {
            this.configuration = configuration;
        }

        @Override
        public ProcessorMetaSupplier readProcessor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(paths, types, new AvroQueryTarget(), predicate, projection);

            SupplierEx<BiFunction<String, GenericRecord, Object[]>> projectionSupplierFn = () -> {
                RowProjector projector = projectorSupplier.get();
                return (key, value) -> projector.project(value);
            };

            return HadoopProcessors.readHadoopP(asSerializable(configuration), projectionSupplierFn);
        }
    }
}
