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

import com.fasterxml.jackson.jr.stree.JrsObject;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;

import java.util.List;
import java.util.Map;

final class JsonMetadataResolver extends MetadataResolver {

    static final JsonMetadataResolver INSTANCE = new JsonMetadataResolver();

    private JsonMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return JsonFileFormat.FORMAT_JSONL;
    }

    @Override
    public List<MappingField> resolveAndValidateFields(List<MappingField> userFields, Map<String, ?> options) {
        return !userFields.isEmpty() ? validateFields(userFields) : resolveFieldsFromSample(options);
    }

    private List<MappingField> validateFields(List<MappingField> userFields) {
        for (MappingField userField : userFields) {
            String path = userField.externalName() == null ? userField.name() : userField.externalName();
            if (path.indexOf('.') >= 0) {
                throw QueryException.error("Invalid field name - '" + path + "'. Nested fields are not supported.");
            }
        }
        return userFields;
    }

    private List<MappingField> resolveFieldsFromSample(Map<String, ?> options) {
        JrsObject object = fetchRecord(FileFormat.json(), options);
        return JsonResolver.resolveFields(object);
    }

    @Override
    public Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, ?> options) {
        return new Metadata(
                toFields(resolvedFields),
                toProcessorMetaSupplier(FileFormat.json(), options),
                JsonQueryTarget::new
        );
    }
}
