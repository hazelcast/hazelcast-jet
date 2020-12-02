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

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.impl.FileProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.impl.FileTraverser;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTIONS;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.util.Map.Entry;

abstract class MetadataResolver {

    abstract String supportedFormat();

    abstract List<MappingField> resolveAndValidateFields(List<MappingField> userFields, Map<String, String> options);

    abstract Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, String> options);

    @SuppressWarnings("unchecked")
    protected <T> T fetchRecord(FileFormat<T> format, Map<String, String> options) {
        FileProcessorMetaSupplier<T> fileProcessorMetaSupplier =
                (FileProcessorMetaSupplier<T>) toProcessorMetaSupplier(format, options);

        try (FileTraverser<T> traverser = fileProcessorMetaSupplier.traverser()) {
            return traverser.next();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    protected List<TableField> toFields(List<MappingField> resolvedFields) {
        return toList(
                resolvedFields,
                field -> new FileTableField(
                        field.name(),
                        field.type(),
                        field.externalName() == null ? field.name() : field.externalName()
                )
        );
    }

    protected <T> ProcessorMetaSupplier toProcessorMetaSupplier(FileFormat<T> format, Map<String, String> options) {
        FileSourceBuilder<?> builder = new FileSourceBuilder<>(options.get(OPTION_PATH))
                .format(format)
                .glob(options.get(OPTION_GLOB))
                .sharedFileSystem(Boolean.parseBoolean(options.get(OPTION_SHARED_FILE_SYSTEM)));
        for (Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!OPTIONS.contains(key)) {
                builder.option(key, value);
            }
        }
        return builder.buildMetaSupplier();
    }
}
