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

import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class CsvMetadataResolver extends MetadataResolver {

    static final CsvMetadataResolver INSTANCE = new CsvMetadataResolver();

    private CsvMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return CsvFileFormat.FORMAT_CSV;
    }

    @Override
    public List<MappingField> resolveAndValidateFields(List<MappingField> userFields, Map<String, ?> options) {
        return !userFields.isEmpty() ? validateFields(userFields) : resolveFieldsFromSample(options);
    }

    private List<MappingField> validateFields(List<MappingField> userFields) {
        for (MappingField userField : userFields) {
            if (userField.externalName() != null) {
                throw QueryException.error("EXTERNAL NAME not supported");
            }
        }
        return userFields;
    }

    private List<MappingField> resolveFieldsFromSample(Map<String, ?> options) {
        String[] header = fetchRecord(FileFormat.csv(false), options);
        return CsvResolver.resolveFields(header);
    }

    @Override
    public Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, ?> options) {
        Map<String, Integer> indicesByNames = new HashMap<>();
        for (int i = 0; i < resolvedFields.size(); i++) {
            MappingField field = resolvedFields.get(i);
            indicesByNames.put(field.name(), i);
        }

        return new Metadata(
                toFields(resolvedFields),
                toProcessorMetaSupplier(FileFormat.csv(true), options),
                () -> new CsvQueryTarget(indicesByNames)
        );
    }
}
