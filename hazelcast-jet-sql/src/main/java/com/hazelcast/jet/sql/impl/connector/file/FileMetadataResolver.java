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
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.JetSqlConnector.AVRO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.JetSqlConnector.CSV_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.JetSqlConnector.TO_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_CHARSET;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.TO_DELIMITER;
import static com.hazelcast.jet.sql.impl.connector.file.FileTargetDescriptors.createAvroDescriptor;
import static com.hazelcast.jet.sql.impl.connector.file.FileTargetDescriptors.createCsvDescriptor;
import static com.hazelcast.jet.sql.impl.connector.file.FileTargetDescriptors.createJsonDescriptor;
import static com.hazelcast.sql.impl.connector.SqlConnector.JSON_SERIALIZATION_FORMAT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

final class FileMetadataResolver {

    private FileMetadataResolver() {
    }

    static FileMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        String format = resolveFormat(options);
        return requireNonNull(resolveMetadata(format, externalFields, options));
    }

    private static String resolveFormat(Map<String, String> options) {
        String format = options.get(TO_SERIALIZATION_FORMAT);
        if (format == null) {
            throw QueryException.error(format("Missing '%s' option", TO_SERIALIZATION_FORMAT));
        }
        return format;
    }

    private static FileMetadata resolveMetadata(
            String format,
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        switch (format) {
            case CSV_SERIALIZATION_FORMAT:
                return resolveCsvMetadata(externalFields, options);
            case JSON_SERIALIZATION_FORMAT:
                return resolveJsonMetadata(externalFields, options);
            case AVRO_SERIALIZATION_FORMAT:
                return resolveAvroMetadata(externalFields);
            default:
                throw QueryException.error(format("Unsupported serialization format - '%s'", format));
        }
    }

    private static FileMetadata resolveCsvMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        // TODO: support schema discovery from headers ???
        if (externalFields.isEmpty()) {
            throw QueryException.error("Empty column list");
        }

        Map<String, Integer> indicesByNames = new HashMap<>();
        List<TableField> fields = new ArrayList<>();

        for (int i = 0; i < externalFields.size(); i++) {
            ExternalField externalField = externalFields.get(i);

            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null) {
                throw QueryException.error("External names are not supported");
            }
            String name = externalField.name();

            TableField field = new FileTableField(name, type);

            indicesByNames.put(field.getName(), i);
            fields.add(field);
        }

        return new FileMetadata(
                fields,
                createCsvDescriptor(
                        options.getOrDefault(TO_CHARSET, UTF_8.name()),
                        options.getOrDefault(TO_DELIMITER, ","),
                        indicesByNames
                )
        );
    }

    private static FileMetadata resolveJsonMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        if (externalFields.isEmpty()) {
            throw QueryException.error("Empty column list");
        }

        List<TableField> fields = new ArrayList<>();

        for (ExternalField externalField : externalFields) {
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null && externalName.chars().filter(ch -> ch == '.').count() > 0) {
                throw QueryException.error(
                        format("Invalid field external name - '%s'. Nested fields are not supported.", externalName)
                );
            }
            String path = externalName == null ? externalField.name() : externalName;
            String name = externalField.name();

            TableField field = new FileTableField(name, type, path);

            fields.add(field);
        }

        return new FileMetadata(
                fields,
                createJsonDescriptor(
                        options.getOrDefault(TO_CHARSET, UTF_8.name())
                )
        );
    }

    private static FileMetadata resolveAvroMetadata(
            List<ExternalField> externalFields
    ) {
        // TODO: support schema discovery from avsc file ???
        // TODO: support schema auto discovery ???
        if (externalFields.isEmpty()) {
            throw QueryException.error("Empty column list");
        }

        List<TableField> fields = new ArrayList<>();

        for (ExternalField externalField : externalFields) {
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null && externalName.chars().filter(ch -> ch == '.').count() > 0) {
                throw QueryException.error(
                        format("Invalid field external name - '%s'. Nested fields are not supported.", externalName)
                );
            }
            String path = externalName == null ? externalField.name() : externalName;
            String name = externalField.name();

            TableField field = new FileTableField(name, type, path);

            fields.add(field);
        }

        return new FileMetadata(
                fields,
                createAvroDescriptor()
        );
    }
}
