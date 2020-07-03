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
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

final class FileTargetDescriptors {

    private FileTargetDescriptors() {
    }

    static FileTargetDescriptor createCsvDescriptor(
            String charset,
            String delimiter,
            Map<String, Integer> indicesByNames
    ) {
        return new CsvFileTargetDescriptor(charset, delimiter, indicesByNames);
    }

    static FileTargetDescriptor createJsonDescriptor(
            String charset
    ) {
        return new JsonFileTargetDescriptor(charset);
    }

    static FileTargetDescriptor createAvroDescriptor() {
        return AvroFileTargetDescriptor.INSTANCE;
    }

    private static class CsvFileTargetDescriptor implements FileTargetDescriptor {

        private final String charset;
        private final String delimiter;
        private final Map<String, Integer> indicesByNames;

        private CsvFileTargetDescriptor(
                String charset,
                String delimiter,
                Map<String, Integer> indicesByNames
        ) {
            this.charset = charset;
            this.delimiter = delimiter;
            this.indicesByNames = indicesByNames;
        }

        @Override
        public FunctionEx<? super Path, ? extends Stream<Object[]>> createReader(
                List<TableField> fields,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            String charset = this.charset;
            String delimiter = this.delimiter;
            Map<String, Integer> indicesByNames = this.indicesByNames;
            String[] paths = fields.stream().map(TableField::getName).toArray(String[]::new);
            QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);

            SupplierEx<RowProjector> projectorSupplier =
                    () -> new RowProjector(new CsvQueryTarget(indicesByNames), paths, types, predicate, projection);

            return path -> {
                RowProjector projector = projectorSupplier.get();

                return Files.lines(path, Charset.forName(charset))
                            .map(line -> line.split(delimiter))
                            .map(projector::project)
                            .filter(Objects::nonNull);
            };
        }
    }

    private static class JsonFileTargetDescriptor implements FileTargetDescriptor {

        private final String charset;

        private JsonFileTargetDescriptor(
                String charset
        ) {
            this.charset = charset;
        }

        @Override
        public FunctionEx<? super Path, ? extends Stream<Object[]>> createReader(
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

            return path -> {
                RowProjector projector = projectorSupplier.get();

                return Files.lines(path, Charset.forName(charset))
                            .map(projector::project)
                            .filter(Objects::nonNull);
            };
        }
    }

    private static class AvroFileTargetDescriptor implements FileTargetDescriptor {

        private static final AvroFileTargetDescriptor INSTANCE = new AvroFileTargetDescriptor();

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
