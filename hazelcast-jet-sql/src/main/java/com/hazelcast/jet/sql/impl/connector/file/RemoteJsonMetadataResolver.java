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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.impl.ReadHadoopNewApiP;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
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

import static com.hazelcast.jet.hadoop.impl.SerializableConfiguration.asSerializable;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.fields;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.paths;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.schema;
import static com.hazelcast.jet.sql.impl.connector.file.JsonMetadataResolver.types;

final class RemoteJsonMetadataResolver implements JsonMetadataResolver {

    private RemoteJsonMetadataResolver() {
    }

    static List<ExternalField> resolveSchema(
            List<ExternalField> externalFields,
            FileOptions options,
            Job job
    ) throws IOException {
        if (!externalFields.isEmpty()) {
            return schema(externalFields);
        } else {
            String line = line(options.path(), job.getConfiguration());
            return schema(line);
        }
    }

    private static String line(String directory, Configuration configuration) throws IOException {
        Path path = new Path(directory);
        try (FileSystem filesystem = path.getFileSystem(configuration)) {
            RemoteIterator<LocatedFileStatus> filesIterator = filesystem.listFiles(path, false); // TODO: directory check, recursive ???
            while (filesIterator.hasNext()) {
                LocatedFileStatus file = filesIterator.next();

                try (Reader input = new InputStreamReader(filesystem.open(file.getPath()));
                     BufferedReader reader = new BufferedReader(input)) {
                    String line = reader.readLine();
                    if (line != null) {
                        return line;
                    }
                }
            }
        }
        throw new IllegalArgumentException("No data found in '" + directory + "'");
    }

    static Metadata resolveMetadata(List<ExternalField> externalFields, FileOptions options, Job job) throws IOException {
        TextInputFormat.addInputPath(job, new Path(options.path()));
        job.setInputFormatClass(TextInputFormat.class);

        List<TableField> fields = fields(externalFields);

        return new Metadata(
                new JsonTargetDescriptor(job.getConfiguration()),
                fields
        );
    }

    private static class JsonTargetDescriptor implements TargetDescriptor {

        private final Configuration configuration;

        private JsonTargetDescriptor(
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
                    () -> new RowProjector(new JsonQueryTarget(), paths, types, predicate, projection);

            SupplierEx<BiFunctionEx<LongWritable, Text, Object[]>> projectionSupplierFn = () -> {
                RowProjector projector = projectorSupplier.get();
                return (position, line) -> projector.project(line.toString());
            };

            return new ReadHadoopNewApiP.MetaSupplier<>(asSerializable(configuration), projectionSupplierFn);
        }
    }
}
