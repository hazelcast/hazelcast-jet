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

import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.jet.sql.SqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.CSV_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static java.util.Objects.requireNonNull;

final class MetadataResolver {

    private static final String HDFS_SCHEMA = "hdfs://";

    private MetadataResolver() {
    }

    static Metadata resolve(
            List<ExternalField> externalFields,
            FileOptions options
    ) {
        try {
            String path = options.path();
            if (path.startsWith(HDFS_SCHEMA)) {
                Job job = Job.getInstance();
                return resolveMetadata(externalFields, options, job);
                // s3, adl, gs, wasbs
            } else {
                return requireNonNull(resolveMetadata(externalFields, options));
            }
        } catch (IOException e) {
            throw QueryException.error("Unable to resolve table metadata : " + e.getMessage(), e);
        }
    }

    private static Metadata resolveMetadata(
            List<ExternalField> externalFields,
            FileOptions options
    ) throws IOException {
        String format = options.format();
        switch (format) {
            case CSV_SERIALIZATION_FORMAT:
                return CsvMetadataResolver.resolve(externalFields, options);
            case JSON_SERIALIZATION_FORMAT:
                return JsonMetadataResolver.resolve(externalFields, options);
            case AVRO_SERIALIZATION_FORMAT:
                return AvroMetadataResolver.resolve(externalFields, options);
            default:
                throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
    }

    private static Metadata resolveMetadata(
            List<ExternalField> externalFields,
            FileOptions options,
            Job job
    ) throws IOException {
        String format = options.format();
        switch (format) {
            case CSV_SERIALIZATION_FORMAT:
                return CsvMetadataResolver.resolve(externalFields, options, job);
            case JSON_SERIALIZATION_FORMAT:
                return JsonMetadataResolver.resolve(externalFields, options, job);
            case AVRO_SERIALIZATION_FORMAT:
                return AvroMetadataResolver.resolve(externalFields, options, job);
            default:
                throw QueryException.error("Unsupported serialization format - '" + format + "'");
        }
    }
}
