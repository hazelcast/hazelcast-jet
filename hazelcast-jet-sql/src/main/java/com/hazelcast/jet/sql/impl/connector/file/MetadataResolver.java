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

import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PARQUET_FORMAT;
import static java.util.Objects.requireNonNull;

// TODO: smarter/cleaner dispatch ?
final class MetadataResolver {

    private static final String HDFS_SCHEMA = "hdfs://";

    private static final String S3_SCHEMA = "s3a://";
    //private static final String S3_ACCESS_KEY = "fs.s3a.access.key";
    //private static final String S3_SECRET_KEY = "fs.s3a.secret.key";

    private MetadataResolver() {
    }

    static List<MappingField> resolveAndValidateFields(List<MappingField> mappingFields, FileOptions options) {
        try {
            String path = options.path();
            if (path.startsWith(HDFS_SCHEMA)) {
                Job job = Job.getInstance();
                return requireNonNull(resolveRemoteFileFields(mappingFields, options, job));
            } else if (path.startsWith(S3_SCHEMA)) {
                Job job = createS3Job(options);
                return requireNonNull(resolveRemoteFileFields(mappingFields, options, job));
                // adl, gs, wasbs
            } else {
                return requireNonNull(resolveLocalFileFields(mappingFields, options));
            }
        } catch (IOException e) {
            throw QueryException.error("Unable to resolve table metadata : " + e.getMessage(), e);
        }
    }

    private static List<MappingField> resolveLocalFileFields(
            List<MappingField> mappingFields,
            FileOptions options
    ) throws IOException {
        String format = options.format();
        switch (format) {
            case CSV_FORMAT:
                return LocalCsvMetadataResolver.resolveFields(mappingFields, options);
            case JSON_FORMAT:
                return LocalJsonMetadataResolver.resolveFields(mappingFields, options);
            case AVRO_FORMAT:
                return LocalAvroMetadataResolver.resolveFields(mappingFields, options);
            default:
                throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
    }

    private static List<MappingField> resolveRemoteFileFields(
            List<MappingField> mappingFields,
            FileOptions options,
            Job job
    ) throws IOException {
        String format = options.format();
        switch (format) {
            case CSV_FORMAT:
                return RemoteCsvMetadataResolver.resolveFields(mappingFields, options, job);
            case JSON_FORMAT:
                return RemoteJsonMetadataResolver.resolveFields(mappingFields, options, job);
            case AVRO_FORMAT:
                return RemoteAvroMetadataResolver.resolveFields(mappingFields, options, job);
            case PARQUET_FORMAT:
                return RemoteParquetMetadataResolver.resolveFields(mappingFields, options, job);
            default:
                throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
    }

    static Metadata resolveMetadata(List<MappingField> mappingFields, FileOptions options) {
        try {
            String path = options.path();
            if (path.startsWith(HDFS_SCHEMA)) {
                Job job = Job.getInstance();
                return requireNonNull(resolveRemoteFileMetadata(mappingFields, options, job));
            } else if (path.startsWith(S3_SCHEMA)) {
                Job job = createS3Job(options);
                return requireNonNull(resolveRemoteFileMetadata(mappingFields, options, job));
                // adl, gs, wasbs
            } else {
                return requireNonNull(resolveLocalFileMetadata(mappingFields, options));
            }
        } catch (IOException e) {
            throw QueryException.error("Unable to resolve table metadata : " + e.getMessage(), e);
        }
    }

    private static Metadata resolveLocalFileMetadata(List<MappingField> mappingFields, FileOptions options) {
        String format = options.format();
        switch (format) {
            case CSV_FORMAT:
                return LocalCsvMetadataResolver.resolveMetadata(mappingFields, options);
            case JSON_FORMAT:
                return LocalJsonMetadataResolver.resolveMetadata(mappingFields, options);
            case AVRO_FORMAT:
                return LocalAvroMetadataResolver.resolveMetadata(mappingFields, options);
            default:
                throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
    }

    private static Metadata resolveRemoteFileMetadata(
            List<MappingField> mappingFields,
            FileOptions options,
            Job job
    ) throws IOException {
        String format = options.format();
        switch (format) {
            case CSV_FORMAT:
                return RemoteCsvMetadataResolver.resolveMetadata(mappingFields, options, job);
            case JSON_FORMAT:
                return RemoteJsonMetadataResolver.resolveMetadata(mappingFields, options, job);
            case AVRO_FORMAT:
                return RemoteAvroMetadataResolver.resolveMetadata(mappingFields, options, job);
            case PARQUET_FORMAT:
                return RemoteParquetMetadataResolver.resolveMetadata(mappingFields, options, job);
            default:
                throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
    }

    private static Job createS3Job(FileOptions options) throws IOException {
        Job job = Job.getInstance();
        //job.getConfiguration().set(S3_ACCESS_KEY, options.s3AccessKey());
        //job.getConfiguration().set(S3_SECRET_KEY, options.s3SecretKey());
        return job;
    }
}
