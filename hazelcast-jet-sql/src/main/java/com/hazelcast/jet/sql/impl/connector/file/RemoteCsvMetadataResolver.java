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
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.hazelcast.jet.hadoop.impl.SerializableConfiguration.asSerializable;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.indices;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.paths;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.resolveFieldsFromSample;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.toTableFields;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.types;
import static com.hazelcast.jet.sql.impl.connector.file.CsvMetadataResolver.validateFields;

final class RemoteCsvMetadataResolver implements CsvMetadataResolver {

    private RemoteCsvMetadataResolver() {
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
            // TODO: ensure options.header() == true ???
            String line = line(options.path(), options.charset(), job.getConfiguration());
            return resolveFieldsFromSample(line, options.delimiter());
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    private static String line(String directory, String charset, Configuration configuration) throws IOException {
        Path path = new Path(directory);
        try (FileSystem filesystem = path.getFileSystem(configuration)) {
            // TODO: directory check, recursive ???
            RemoteIterator<LocatedFileStatus> filesIterator = filesystem.listFiles(path, false);
            while (filesIterator.hasNext()) {
                LocatedFileStatus file = filesIterator.next();

                try (
                        Reader input = new InputStreamReader(filesystem.open(file.getPath()), charset);
                        BufferedReader reader = new BufferedReader(input)
                ) {
                    String line = reader.readLine();
                    if (line != null) {
                        return line;
                    }
                }
            }
        }
        throw new IllegalArgumentException("No data found in '" + directory + "'");
    }

    static Metadata resolveMetadata(List<MappingField> mappingFields, FileOptions options, Job job) throws IOException {
        List<TableField> fields = toTableFields(mappingFields);

        TextInputFormat.addInputPath(job, new Path(options.path()));
        job.setInputFormatClass(TextInputFormat.class);

        return new Metadata(
                new CsvTargetDescriptor(options.delimiter(), options.header(), job.getConfiguration()),
                fields
        );
    }

    private static final class CsvTargetDescriptor implements TargetDescriptor {

        private final String delimiter;
        private final boolean header;
        private final Configuration configuration;

        private CsvTargetDescriptor(
                String delimiter,
                boolean header,
                Configuration configuration
        ) {
            this.delimiter = delimiter;
            this.header = header;
            this.configuration = configuration;
        }

        @Override
        public ProcessorMetaSupplier readProcessor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String delimiter = this.delimiter;
            boolean header = this.header;
            Map<String, Integer> indicesByNames = indices(fields);
            String[] paths = paths(fields);
            QueryDataType[] types = types(fields);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(paths, types, new CsvQueryTarget(indicesByNames), predicate, projection);

            SupplierEx<BiFunction<LongWritable, Text, Object[]>> projectionSupplierFn = () -> {
                RowProjector projector = projectorSupplier.get();
                return (position, line) -> {
                    if (position.get() == 0 && header) {
                        return null;
                    } else {
                        return projector.project(line.toString().split(delimiter));
                    }
                };
            };

            return HadoopProcessors.readHadoopP(asSerializable(configuration), projectionSupplierFn);
        }
    }
}
