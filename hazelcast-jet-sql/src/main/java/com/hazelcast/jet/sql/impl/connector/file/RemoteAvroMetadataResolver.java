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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

import static com.hazelcast.jet.hadoop.impl.SerializableConfiguration.asSerializable;
import static com.hazelcast.jet.sql.impl.connector.file.AvroMetadataResolver.paths;
import static com.hazelcast.jet.sql.impl.connector.file.AvroMetadataResolver.resolveFieldsFromSchema;
import static com.hazelcast.jet.sql.impl.connector.file.AvroMetadataResolver.toTableFields;
import static com.hazelcast.jet.sql.impl.connector.file.AvroMetadataResolver.types;
import static com.hazelcast.jet.sql.impl.connector.file.AvroMetadataResolver.validateFields;

final class RemoteAvroMetadataResolver implements AvroMetadataResolver {

    private RemoteAvroMetadataResolver() {
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
            Schema schema = findAvroSchema(options.path(), job.getConfiguration());
            return resolveFieldsFromSchema(schema);
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    private static Schema findAvroSchema(String directory, Configuration configuration) throws IOException {
        Path path = new Path(directory);
        try (FileSystem filesystem = path.getFileSystem(configuration)) {
            // TODO: directory check, recursive ???
            RemoteIterator<LocatedFileStatus> filesIterator = filesystem.listFiles(path, false);
            if (filesIterator.hasNext()) {
                LocatedFileStatus file = filesIterator.next();

                try (
                        DataFileStream<GenericRecord> stream =
                                new DataFileStream<>(filesystem.open(file.getPath()), new GenericDatumReader<>())
                ) {
                    return stream.getSchema();
                }
            }
        }
        throw new IllegalArgumentException("No data found in '" + directory + "'");
    }

    static Metadata resolveMetadata(List<MappingField> mappingFields, FileOptions options, Job job) throws IOException {
        List<TableField> fields = toTableFields(mappingFields);

        AvroKeyInputFormat.addInputPath(job, new Path(options.path()));
        job.setInputFormatClass(AvroKeyInputFormat.class);

        return new Metadata(
                new AvroTargetDescriptor(job.getConfiguration()),
                fields
        );
    }

    private static final class AvroTargetDescriptor implements TargetDescriptor {

        private final Configuration configuration;

        private AvroTargetDescriptor(
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

            SupplierEx<BiFunction<AvroKey<GenericRecord>, NullWritable, Object[]>> projectionSupplierFn = () -> {
                RowProjector projector = projectorSupplier.get();
                return (key, value) -> projector.project(key.datum());
            };

            return HadoopProcessors.readHadoopP(asSerializable(configuration), projectionSupplierFn);
        }
    }
}
