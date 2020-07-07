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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.JetSqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.JetSqlConnector.CSV_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.JetSqlConnector.TO_SERIALIZATION_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class MetadataResolver {

    private MetadataResolver() {
    }

    static Metadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            String directory,
            String glob
    ) {
        try {
            String format = resolveFormat(options);
            return requireNonNull(resolveMetadata(format, externalFields, options, directory, glob));
        } catch (IOException e) {
            throw QueryException.error("Unable to resolve table metadata : " + e.getMessage(), e);
        }
    }

    private static String resolveFormat(Map<String, String> options) {
        String format = options.get(TO_SERIALIZATION_FORMAT);
        if (format == null) {
            throw QueryException.error(format("Missing '%s' option", TO_SERIALIZATION_FORMAT));
        }
        return format;
    }

    private static Metadata resolveMetadata(
            String format,
            List<ExternalField> externalFields,
            Map<String, String> options,
            String directory,
            String glob
    ) throws IOException {
        switch (format) {
            case CSV_SERIALIZATION_FORMAT:
                return CsvMetadataResolver.resolve(externalFields, options, directory, glob);
            case JSON_SERIALIZATION_FORMAT:
                return JsonMetadataResolver.resolve(externalFields, options, directory, glob);
            case AVRO_SERIALIZATION_FORMAT:
                return AvroMetadataResolver.resolve(externalFields, directory, glob);
            default:
                throw QueryException.error(format("Unsupported serialization format - '%s'", format));
        }
    }
}
