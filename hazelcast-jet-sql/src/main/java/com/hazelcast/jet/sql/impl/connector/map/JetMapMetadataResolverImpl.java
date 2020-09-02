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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.SqlConnector.JAVA_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.SqlConnector.PORTABLE_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_SERIALIZATION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_FACTORY_ID;
import static java.util.Collections.emptyList;

// TODO: refactor
public class JetMapMetadataResolverImpl implements JetMapMetadataResolver {

    private final InternalSerializationService serializationService;

    public JetMapMetadataResolverImpl(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Object resolveClass(Class<?> clazz, boolean key) {
        Map<String, String> options = new HashMap<>();
        options.put(key ? OPTION_SERIALIZATION_KEY_FORMAT
                : OPTION_SERIALIZATION_VALUE_FORMAT, JAVA_SERIALIZATION_FORMAT);
        options.put(key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS, clazz.getName());

        List<MappingField> mappingFields = JavaEntryMetadataResolver.INSTANCE.resolveFields(
                emptyList(),
                options,
                key,
                serializationService
        );
        EntryMetadata metadata = JavaEntryMetadataResolver.INSTANCE.resolveMetadata(
                mappingFields,
                options,
                key,
                serializationService
        );
        return metadata.getUpsertTargetDescriptor();
    }

    @Override
    public Object resolvePortable(ClassDefinition clazz, boolean key) {
        Map<String, String> options = new HashMap<>();
        options.put(
                key ? OPTION_SERIALIZATION_KEY_FORMAT : OPTION_SERIALIZATION_VALUE_FORMAT, PORTABLE_SERIALIZATION_FORMAT
        );
        options.put(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(clazz.getFactoryId()));
        options.put(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(clazz.getClassId()));
        options.put(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, String.valueOf(clazz.getVersion()));

        List<MappingField> mappingFields = PortableEntryMetadataResolver.INSTANCE.resolveFields(
                emptyList(),
                options,
                key,
                serializationService
        );
        EntryMetadata metadata = PortableEntryMetadataResolver.INSTANCE.resolveMetadata(
                mappingFields,
                options,
                key,
                serializationService
        );
        return metadata.getUpsertTargetDescriptor();
    }
}
