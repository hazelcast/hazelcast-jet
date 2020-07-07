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
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

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

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.String.format;

final class AvroMetadataResolver {

    private AvroMetadataResolver() {
    }

    static Metadata resolve(
            List<ExternalField> externalFields,
            String directory,
            String glob
    ) throws IOException {
        return !externalFields.isEmpty()
                ? resolveFromFields(externalFields)
                : resolveFromSample(directory, glob);
    }

    private static Metadata resolveFromFields(
            List<ExternalField> externalFields
    ) {
        List<TableField> fields = new ArrayList<>();

        for (ExternalField externalField : externalFields) {
            String name = externalField.name();
            QueryDataType type = externalField.type();

            String externalName = externalField.externalName();
            if (externalName != null && externalName.chars().filter(ch -> ch == '.').count() > 0) {
                throw QueryException.error(
                        format("Invalid field external name - '%s'. Nested fields are not supported.", externalName)
                );
            }
            String path = externalName == null ? externalField.name() : externalName;

            TableField field = new FileTableField(name, type, path);

            fields.add(field);
        }

        return new Metadata(fields, AvroTargetDescriptor.INSTANCE);
    }

    private static Metadata resolveFromSample(
            String directory,
            String glob
    ) throws IOException {
        Schema schema = schema(directory, glob);
        if (schema == null) {
            throw new IllegalArgumentException("No data found in '" + directory + "/" + glob + "'");
        }

        Map<String, TableField> fields = new HashMap<>();

        for (Schema.Field avroField : schema.getFields()) {
            String name = avroField.name();
            QueryDataType type = resolveType(avroField.schema().getType());

            TableField field = new FileTableField(name, type);

            fields.putIfAbsent(field.getName(), field);
        }

        return new Metadata(new ArrayList<>(fields.values()), AvroTargetDescriptor.INSTANCE);
    }

    private static Schema schema(
            String directory,
            String glob
    ) throws IOException {
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

    private static class AvroTargetDescriptor implements TargetDescriptor {

        private static final AvroTargetDescriptor INSTANCE = new AvroTargetDescriptor();

        @Override
        public FunctionEx<? super Path, ? extends Stream<Object[]>> createReader(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            // TODO: get rid of casting ???
            String[] paths = fields.stream().map(field -> ((FileTableField) field).getPath()).toArray(String[]::new);
            QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new AvroQueryTarget(), paths, types, predicate, projection);

            return path -> {
                RowProjector projector = projectorSupplier.get();

                DataFileReader<GenericRecord> reader = new DataFileReader<>(path.toFile(), new GenericDatumReader<>());
                return StreamSupport.stream(reader.spliterator(), false)
                                    .map(projector::project)
                                    .filter(Objects::nonNull)
                                    .onClose(() -> uncheckRun(reader::close));
            };
        }
    }
}
