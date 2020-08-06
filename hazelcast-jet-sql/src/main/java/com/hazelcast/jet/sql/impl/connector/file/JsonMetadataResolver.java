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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonObject.Member;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.jet.sql.impl.schema.ExternalField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

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
import java.util.stream.Stream;

final class JsonMetadataResolver {

    private JsonMetadataResolver() {
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
            String fieldPath = externalName == null ? externalField.name() : externalName;

            TableField field = new FileTableField(name, type, fieldPath);

            fields.add(field);
        }

        return new Metadata(new JsonTargetDescriptor(path, glob, sharedFileSystem, charset), fields);
    }

    private static Metadata resolveFromSample(FileOptions options) throws IOException {
        String path = options.path();
        String glob = options.glob();
        boolean sharedFileSystem = options.sharedFileSystem();
        String charset = options.charset();

        String line = line(path, glob);
        if (line == null) {
            throw new IllegalArgumentException("No data found in '" + path + "/" + glob + "'");
        }

        Map<String, TableField> fields = new HashMap<>();

        JsonObject object = Json.parse(line).asObject();
        for (Member member : object) {
            String name = member.getName();
            QueryDataType type = resolveType(member.getValue());

            TableField field = new FileTableField(name, type);

            fields.putIfAbsent(field.getName(), field);
        }

        return new Metadata(
                new JsonTargetDescriptor(path, glob, sharedFileSystem, charset),
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

    private static QueryDataType resolveType(JsonValue value) {
        if (value.isBoolean()) {
            return QueryDataType.BOOLEAN;
        } else if (value.isNumber()) {
            return QueryDataType.DOUBLE;
        } else if (value.isString()) {
            return QueryDataType.VARCHAR;
        } else {
            return QueryDataType.OBJECT;
        }
    }

    private static final class JsonTargetDescriptor implements TargetDescriptor {

        private final String path;
        private final String glob;
        private final boolean sharedFileSystem;
        private final String charset;

        private JsonTargetDescriptor(
                String path,
                String glob,
                boolean sharedFileSystem,
                String charset
        ) {
            this.path = path;
            this.glob = glob;
            this.sharedFileSystem = sharedFileSystem;
            this.charset = charset;
        }

        @Override
        public ProcessorMetaSupplier processor(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            // TODO: get rid of casting ???
            String[] paths = fields.stream().map(field -> ((FileTableField) field).getPath()).toArray(String[]::new);
            QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new JsonQueryTarget(), paths, types, predicate, projection);

            FunctionEx<? super Path, ? extends Stream<Object[]>> readFileFn = path -> {
                RowProjector projector = projectorSupplier.get();

                return Files.lines(path, Charset.forName(charset))
                            .map(projector::project)
                            .filter(Objects::nonNull);
            };

            return ReadFilesP.metaSupplier(path, glob, sharedFileSystem, readFileFn);
        }
    }
}
